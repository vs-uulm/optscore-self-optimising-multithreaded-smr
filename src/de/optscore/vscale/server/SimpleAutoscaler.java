package de.optscore.vscale.server;

import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.util.MeanVarianceSampler;
import de.uniulm.vs.art.uds.UDSLock;
import de.uniulm.vs.art.uds.UDScheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class implements a simple autoscaling mechanism for UDS primaries. Using ByTI data, it
 * deterministically decides when to scale UDS primaries (and potentially also CPU cores, ...) up or down.
 *
 * The algorithm is very basic, and simply looks for changes in the current load as reported by the number of
 * requests seen by the system in a ByTI, and depending on the kind of changes and the current configuration, tries
 * to reconfigure to find a new, better configuration.
 *
 * The algorithm is triggered by the decide()-method in the ByTIManager, i.e. whenever a ByTI has been closed and UDS
 * has deterministically scheduled the decision.
 */
public class SimpleAutoscaler implements AutoScaler {

    private static final Logger logger = Logger.getLogger(SimpleAutoscaler.class.getName());

    private final ScalingBand[] scalingBands;
    private ScalingBand currentBand;

    private boolean attemptedScaleUp;
    private int newBandCountdown;

    private final double bandAgingFactor = 0.95d;
    private final UDSLock scalingLock;

    public SimpleAutoscaler() {
        this(8, new UDSLock(555));
    }

    public SimpleAutoscaler(int maxPrimaries, UDSLock scalingLock) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.scalingBands = new ScalingBand[maxPrimaries];
        this.currentBand = null;
        this.attemptedScaleUp = false;
        this.newBandCountdown = 0;
        this.scalingLock = scalingLock;
    }

    /**
     * Main method being called every time a ByTI decision has been made. This method will check whether scaling
     * should be attempted, then attempt it and see what happens in the next ByTI.
     *
     * @param byTIId The ID of the decided ByTI that triggered this call
     * @param reqCounter The number of requests in said ByTI
     * @param imprecise whether the ByTI was imprecise
     */
    public void decideScaling(int byTIId, int reqCounter, boolean imprecise) {
        scalingLock.lock();
        try {

            int currentPrimaries = UDScheduler.getInstance().getCurrentUDSConfigurationPrimaries();
            boolean scaledUp = false;

            // First check whether we are currently in a scaling band
            if(currentBand == null) {
                // initial case, we haven't scaled or decided anything yet
                // check whether a scaling band for the currently configured primaries already exists (likely not)
                if(scalingBands[currentPrimaries - 1] == null) {
                    // create new ScalingBand for this configuration
                    scalingBands[currentPrimaries - 1] = new ScalingBand(currentPrimaries, reqCounter);
                    // wait at least 5 ByTIs before deciding something again
                    newBandCountdown = 4;
                    if(logger.isLoggable(Level.FINE)) {
                        logger.fine("SimpleAutoScaler created new ScalingBand for " + currentPrimaries + " primaries");
                    }
                }
                currentBand = scalingBands[currentPrimaries - 1];
            }

            // Only do any scaling stuff if we didn't newly create a band recently
            if(newBandCountdown > 0) {
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(loggerPrefix() + "status: recently created and changed to new band, waiting " +
                            newBandCountdown + " ByTIs before deciding anything ...");
                }
                // add the current value to this new band, to get a baseline value of what it is capable of
                currentBand.newValue(byTIId, reqCounter);
                newBandCountdown--;
                return;
            }

            // "degrade" the highest seen values in all bands
            ageAllBands(bandAgingFactor);

            // Now check whether we need to scale down because of a loss of throughput. Only try if we aren't already at
            // the lowest band
            if(logger.isLoggable(Level.FINE)) {
                logger.fine(loggerPrefix() + "status: Checking reqCounter (" + reqCounter + ") against current bands' " +
                        "values");
            }
            if(currentBand.getPrimaries() > 1 &&
                    reqCounter < (currentBand.getMeanVarianceSampler().getMean() / 4)) {
                // we lost significant throughput. This can be either due to a recent attempt of scaling up or because
                // clients disconnected
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(loggerPrefix() + "detected drop in throughput: " + reqCounter + " reqs in ByTI");
                }
                if(attemptedScaleUp) {
                    // we come from an attempted upscaling, so let's reduce primaries by 1 and hope that suffices
                    currentPrimaries = currentPrimaries - 1;
                    // the band below the current one should definitely exist, since we can only come here through that
                    currentBand = scalingBands[currentPrimaries - 1];
                    // reconfigure UDS
                    UDScheduler.getInstance().requestReconfigurationPrimaries(currentPrimaries);
                    // and don't save the current reqCounter, we don't know which band it should belong to (yet) TODO
                    if(logger.isLoggable(Level.FINE)) {
                        logger.fine(loggerPrefix() + "scaled down by 1 primary after attempted upScaling");
                    }
                } else {
                    // we didn't scale up last time, so it is likely clients disconnected --> go to base configuration
                    // immediately (e.g. 1 primary)
                    currentPrimaries = 1;
                    if(scalingBands[currentPrimaries - 1] == null) {
                        scalingBands[currentPrimaries - 1] = new ScalingBand(currentPrimaries, reqCounter);
                        newBandCountdown = 2;
                    }
                    currentBand = scalingBands[currentPrimaries - 1];
                    // reconfigure UDS
                    UDScheduler.getInstance().requestReconfigurationPrimaries(currentPrimaries);
                    // and don't save the reqCounter at all, we don't know which band it best belongs to atm ... TODO
                    if(logger.isLoggable(Level.FINE)) {
                        logger.fine(loggerPrefix() + "scaled down to 1 primary after sudden drop in throughput");
                    }
                }

                // Check whether we should try scaling up. Only if we didn't scale up last byTI.
            } else if(!attemptedScaleUp &&
                    reqCounter > (currentBand.getMaxStableReqCounter() + (currentBand.getMeanVarianceSampler().getVariance() / 1.35))) {

                // Add the current reqCounter to the current band before scaling, because apparently the current
                // band is capable of at least this many reqs per ByTI
                currentBand.newValue(byTIId, reqCounter);

                // now try scaling up
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(loggerPrefix() + "detected rise in throughput: " + reqCounter + " reqs in ByTI");
                }
                if(currentPrimaries == scalingBands.length) {
                    // we are already at max primaries, can't do anything
                    if(logger.isLoggable(Level.FINE)) {
                        logger.fine(loggerPrefix() + "can not scale up any further. Staying in band.");
                    }
                } else {
                    // we can scale up into the next band
                    currentPrimaries = currentPrimaries + 1;
                    if(currentPrimaries <= scalingBands.length && scalingBands[currentPrimaries - 1] == null) {
                        // we haven't been up to this band yet, create it using the current reqCounter (TODO is this smart?)
                        scalingBands[currentPrimaries - 1] = new ScalingBand(currentPrimaries, reqCounter);
                        newBandCountdown = 2;
                    }
                    // then switch to the new band ...
                    currentBand = scalingBands[currentPrimaries - 1];
                    // and reconfigure UDS ...
                    UDScheduler.getInstance().requestReconfigurationPrimaries(currentPrimaries);
                    // and remember we scaled up so the next time this is called we can react if things go wrong.
                    scaledUp = true;
                    if(logger.isLoggable(Level.FINE)) {
                        logger.fine(loggerPrefix() + "attempts scaling up to " + currentPrimaries + " primaries ...");
                    }
                }
            } else {
                // we do not need to scale, so just add the current reqCounter as a new value to the band
                currentBand.newValue(byTIId, reqCounter);
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(loggerPrefix() + "updated current bands' values");
                }
            }

            this.attemptedScaleUp = scaledUp;
        } finally {
            scalingLock.unlock();
        }
    }

    /**
     * Iterates through all currently existing bands and reduces the highest seen values by multiplying the max with
     * agingFactor
     * @param agingFactor By how much to degrade the values in all bands by multiplying it with the current max.: Should
     *                   be between 0-1
     */
    private void ageAllBands(double agingFactor) {
        if(agingFactor < 0 || agingFactor > 1) {
            throw new IllegalArgumentException("ScalingBand agingFactor has to be between 0 and 1");
        }

        for(ScalingBand band : scalingBands) {
            if(band != null) {
                band.ageMaxValues(agingFactor);
            }
        }
    }

    private String loggerPrefix() {
        return "SimpleAutoscaler (" + currentBand.getPrimaries() + ";"
                + currentBand.maxStableReqCounter + ";"
                + currentBand.getMeanVarianceSampler().getMean() + ";"
                + currentBand.getMeanVarianceSampler().getStdDev() + ") ";
    }


    /**
     * Models a virtual horizontal "band" in an imagined graph of throughput vs response time, to remember values
     * like avgReqCounter, maxReqCounter, sdReqCounter, etc... for the current configuration.
     */
    private static class ScalingBand {

        private final int primaries;

        private int maxStableReqCounter;
        private MeanVarianceSampler meanVarianceSampler;
        private final int[] requestWindow;
        private int index;

        ScalingBand(int primaries, int initialValue) {
            this.primaries = primaries;
            this.maxStableReqCounter = 0;
            meanVarianceSampler = new MeanVarianceSampler();
            this.requestWindow = new int[5];
            this.index = 0;

            for(int i = 0; i < requestWindow.length; i++) {
                requestWindow[i] = initialValue;
                meanVarianceSampler.add(initialValue);
            }
        }

        /**
         * Updates the values for this band
         *
         * @param reqCounter the number of requests in the current ByTI
         */
        void newValue(int byTIId, int reqCounter) {
            // update the sliding window and MeanVarianceSampler for the sliding window
            int i = index++;
            // remember old value to replace in MeanVarianceSampler
            int oldVal = requestWindow[i % requestWindow.length];
            // save the new responseTime (i.e. slide window)
            requestWindow[i % requestWindow.length] = reqCounter;
            // replace the old value in the MeanVarianceSampler so all values are updated to reflect the requestWindow
            meanVarianceSampler.replace(oldVal, reqCounter);

            // and update the maxStableReqCounter if the new value is bigger than the current one
            maxStableReqCounter = Math.max(reqCounter, maxStableReqCounter);
        }

        /**
         * Reduce the maxStableReqCounter by multiplying it with agingFactor (between [0-1])
         * @param agingFactor Factor between 0-1 which is used for "aging" this band's values
         */
        void ageMaxValues(double agingFactor) {
            if(agingFactor < 0 || agingFactor > 1) {
                throw new IllegalArgumentException();
            }

            maxStableReqCounter = (int) Math.floor(maxStableReqCounter * agingFactor);

        }

        int getPrimaries() {
            return primaries;
        }

        int getMaxStableReqCounter() {
            return maxStableReqCounter;
        }

        MeanVarianceSampler getMeanVarianceSampler() {
            return meanVarianceSampler;
        }
    }

}
