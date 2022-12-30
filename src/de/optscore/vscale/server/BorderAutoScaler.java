package de.optscore.vscale.server;

import de.uniulm.vs.art.uds.UDSLock;
import de.uniulm.vs.art.uds.UDScheduler;

import java.util.Arrays;
import java.util.logging.Logger;

public class BorderAutoScaler implements AutoScaler {
    private static final Logger logger = Logger.getLogger(BorderAutoScaler.class.getName());

    private int[] maxSeenRequestsPerPrim;
    private double b;
    private boolean recentlyReconfigured;
    private int throughputStableCounter;

    // the frequency and amount of aging b, i.e. periodically increasing it to avoid it becoming ever smaller
    private final int agingFrequency = 15;
    private final double agingFactor = 1.1d;

    // How often the algorithm should accept a reqCounter between (scalingMargin * max[p]) and max[p] before scaling up
    private final int scalingTimeFactor = 7;
    private final double scalingMargin = 0.9;
    private final double scalingFactor = 0.9;

    private final UDSLock scalingLock;

    public BorderAutoScaler() {
        this(8, new UDSLock(555));
    }

    public BorderAutoScaler(int maxPrimaries, UDSLock scalingLock) {
        this.maxSeenRequestsPerPrim = new int[maxPrimaries];
        Arrays.fill(this.maxSeenRequestsPerPrim, 0);
        // arbitrarily chosen high value for b (much larger than usual r)
        this.b = 1000000;
        this.recentlyReconfigured = false;
        this.throughputStableCounter = 0;
        this.scalingLock = scalingLock;
    }

    /**
     * Main method being called every time a ByTI decision has been made. This method will update the internal
     * AutoScaler variable values and potentially decide to reconfigure/scale.
     *
     * @param byTIId The id of the ByTI that was just decided
     * @param reqCounter The number of requests counted in the ByTI ("r")
     * @param imprecise currently unused
     */
    public void decideScaling(int byTIId, int reqCounter, boolean imprecise) {
        scalingLock.lock();
        try {

            int currentPrimaries = UDScheduler.getInstance().getCurrentUDSConfigurationPrimaries();

            // if there was a reconfiguration in the last ByTI, discard this ByTI measurement and continue in the next ByTI
            if(recentlyReconfigured) {
                this.recentlyReconfigured = false;
                return;
            }

            // now first age b and reset maxCounters according to agingFrequency
            if(byTIId % agingFrequency == 0) {
                this.b = (int) (agingFactor * this.b);
                for(int i = 0; i < maxSeenRequestsPerPrim.length; i++) {
                    maxSeenRequestsPerPrim[i] = (int) (maxSeenRequestsPerPrim[i] / agingFactor);
                }
            }

            // then check if b has to be updated
            if(reqCounter > maxSeenRequestsPerPrim[currentPrimaries - 1]) {
                // update maxSeenRequestsPerPrim for this primary count, then reset the stableCounter, return immediately
                maxSeenRequestsPerPrim[currentPrimaries - 1] = reqCounter;
                this.throughputStableCounter = 0;
                return;
            } else if(reqCounter > this.scalingMargin * maxSeenRequestsPerPrim[currentPrimaries - 1]) {
                // reqCounter is between 0.9max[p] and max[p]. Increase the count of how often this happened already
                this.throughputStableCounter++;
                // if we have been within the scalingMargin for scalingTimeFactor times, then re-calculate b
                if(this.throughputStableCounter >= this.scalingTimeFactor) {
                    this.b = (this.scalingFactor * maxSeenRequestsPerPrim[currentPrimaries - 1]
                            * 2 / currentPrimaries);
                }
            } else {
                this.throughputStableCounter = 0;
            }

            // then see whether we are at a border and should reconfigure to a new number of primaries
            int newP = Math.min(Math.max(1, (int) Math.ceil(2 * reqCounter / this.b)), maxSeenRequestsPerPrim.length);
            if(newP != currentPrimaries) {
                // reconfigure
                UDScheduler.getInstance().requestReconfigurationPrimaries(newP);
                this.throughputStableCounter = 0;
                this.recentlyReconfigured = true;
            }
        } finally {
            scalingLock.unlock();
        }
    }

}
