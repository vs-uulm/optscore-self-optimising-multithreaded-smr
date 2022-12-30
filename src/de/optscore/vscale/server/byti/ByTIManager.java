package de.optscore.vscale.server.byti;

import bftsmart.tom.MessageContext;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.server.AutoScaler;
import de.optscore.vscale.server.BorderAutoScaler;
import de.optscore.vscale.server.EvalServer;
import de.optscore.vscale.util.BufferedStatsWriter;
import de.uniulm.vs.art.uds.UDSLock;
import de.uniulm.vs.art.uds.UDScheduler;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Manages a ByTIClient, i.e. adjusts its rate depending on BClockIntervals,
 * calculates new rates, handles intervals, etc.
 */
public class ByTIManager {
    private static final Logger logger = Logger.getLogger(ByTIManager.class.getName());

    private final BufferedStatsWriter byTIStatsWriter;
    private final BufferedStatsWriter rawByTIStatsWriter;

    private ByTIClient byTIClient;
    private final ScheduledExecutorService executorService;

    private int currentByTITickrateMs;
    private final int maxRoundLatencyMs;

    private int m;
    private int firstReqInByTINo;
    private int lastReqInByTINo;
    private final TreeMap<Integer, Integer> replicaTicks;
    private final TreeMap<Integer, Integer> tickReqCounters;
    private int goodReplicas;
    private int badReplicas;
    private boolean imprecise;

    private int localReqCounter;
    private int decidedReqCounter;

    private int byTIId;
    private int tickId;
    private long byTICloseTimeNs;
    private long previousByTITickTimeNs;

    private final AtomicBoolean byTIStarted;

    private final AutoScaler autoScaler;

    public ByTIManager(int blClientPid, BufferedStatsWriter byTIStatsWriter, BufferedStatsWriter rawByTIStatsWriter,
                       boolean maliciousClient) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
        this.byTIStatsWriter = byTIStatsWriter;
        this.rawByTIStatsWriter = rawByTIStatsWriter;

        this.byTIClient = new ByTIClient(blClientPid, this, maliciousClient);
        this.executorService = Executors.newSingleThreadScheduledExecutor();

        this.byTIId = 0;
        this.tickId = 0;
        this.byTICloseTimeNs = System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET;
        this.previousByTITickTimeNs = byTICloseTimeNs;

        // config
        this.m = 1;
        this.currentByTITickrateMs = 100;
        // TODO unused config (for the moment)
        this.maxRoundLatencyMs = 40;
        //this.currentByTITickrateMs = calculateBClockIntervalMs();

        this.replicaTicks = new TreeMap<>();
        this.tickReqCounters = new TreeMap<>();
        this.lastReqInByTINo = -1;
        this.firstReqInByTINo = 0;
        this.imprecise = true;

        this.localReqCounter = 0;
        this.decidedReqCounter = 0;

        this.byTIStarted = new AtomicBoolean(false);

        this.autoScaler = new BorderAutoScaler(7, new UDSLock(555));
    }

    /**
     * Should be called every time a request is received by the replica.
     * Determines whether the request is a BClockRequest, and updates the ByzantineTimeInterval accordingly
     *
     * @param actionType The type of the request received
     * @param msgCtx     The messageContext of the request that was received
     */
    public boolean requestReceived(EvalActionType actionType, int tickReqCounter, MessageContext msgCtx) {
        if(actionType == EvalActionType.ByTI) {
            // it's a ByTI Tick
            long now = System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET;
            this.tickId++;
            rawByTIStatsWriter.writeByTITick(now,
                    msgCtx.getSender(),
                    tickId,
                    tickReqCounter,
                    msgCtx.getConsensusId());
            return replicaTickReceived(msgCtx, tickReqCounter, now);
        } else {
            localReqCounter++;
            return false;
        }


    }

    /**
     * Runs the ByTI algorithm
     * @param msgCtx
     * @return true when there is a decision
     */
    private boolean replicaTickReceived(MessageContext msgCtx, int tickReqCounter, long tickTimeNs) {
        // TODO remove; workaround to flush data to disk more often. Do proper auto flush before replicashutdown
        if(tickId % 4 == 0) {
            byTIStatsWriter.flush();
            rawByTIStatsWriter.flush();
        }

        int N = byTIClient.getViewManager().getCurrentViewN();
        int f = byTIClient.getViewManager().getCurrentViewF();

        // increment tickCounter for this replica
        int replicaTickCount = replicaTicks.merge(msgCtx.getSender(), 1, Integer::sum);
        imprecise = false;

        // remember the localReqCounter included in this tick, overwriting old value if present
        tickReqCounters.put(msgCtx.getSender(), tickReqCounter);

        // If a replica sent exactly m tickRequests, it is tentatively finished for this interval
        if(replicaTickCount == m) {
            goodReplicas++;
            // if we have at least N-f tentatively finished (aka good) replicas, we can tentatively close the interval
            if(goodReplicas >= N - f) {
                lastReqInByTINo = tickId;
                byTICloseTimeNs = tickTimeNs;
            }

        }

        // if a replica has sent m+1 tickRequests (=> badReplica) in this interval it is marked as bad
        if(replicaTickCount == m + 1) {
            goodReplicas--;
            badReplicas++;
            imprecise = badReplicas > f;
        }

        // we check whether there are enough good replicas to immediately close
        // or there are more than f badReplicas (which means the interval will be closed & imprecise)
        // or still more than N-f-1 other good replicas (which means the interval can be closed where it was
        // tentatively closed before)
        if(goodReplicas == N || (replicaTickCount == m + 1 && (imprecise || goodReplicas >= N - f - 1))) {

            // reset the replicaTicks and replica niceness counters
            replicaTicks.clear();
            goodReplicas = 0;
            badReplicas = 0;

            // if we haven't yet seen enough good replicas in this interval, (i.e. there have to be less than N-f
            // good replicas and more than f badReplicas), the interval can never be closed correctly and we
            // remember the globalSeqNo of the previously received message
            if(lastReqInByTINo == -1 || replicaTickCount == m + 1) {  // oscillation "fix"?
                lastReqInByTINo = tickId - 1;
                byTICloseTimeNs = previousByTITickTimeNs;
                // also set this tick request to be the first of the newly begun next interval
                replicaTicks.put(msgCtx.getSender(), 1);
                if(m == 1) {
                    goodReplicas = 1;
                }
            }

            // remember previousTickTime
            this.previousByTITickTimeNs = tickTimeNs;

            // deterministically choose a localReqCounter value from the received reqCounters of all replicas
            List<Integer> reqCounters = tickReqCounters.values().stream().sorted().collect(Collectors.toList());
            // remove the top and bottom f values
            for(int i = 0; i < f; i++) {
                reqCounters.remove(0);
                reqCounters.remove(reqCounters.size() - 1);
            }
            // average the remaining values and cast to int to get the final value
            if(reqCounters.size() > 0) {
                decidedReqCounter = (int) reqCounters.stream().mapToInt(x -> x).average().getAsDouble();
            }

            // reset tickReqCounters for next interval
            tickReqCounters.clear();

            // we close the interval in any case now (enough good replicas or not, imprecise or not), so decide
            return true;
        }

        // can't close the interval yet, keep going
        // remember previousTickTime
        this.previousByTITickTimeNs = tickTimeNs;
        return false;
    }

    public void prepareNextInterval() {
        // reset counters/stats for next interval and increment interval counter
        byTIId++;
        firstReqInByTINo = lastReqInByTINo + 1;
        lastReqInByTINo = -1;
        decidedReqCounter = -1;
        imprecise = true;
    }

    public void decide(int byTIId, int firstNo, int lastNo, int reqCounter, boolean imprecise, long byTICloseTimeNs) {

        // log the decision so that we can analyse lengths, etc
        byTIStatsWriter.writeByTIClosed(System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET,
                byTIId,
                firstNo,
                lastNo,
                reqCounter,
                UDScheduler.getInstance().getCurrentUDSConfigurationPrimaries(),
                imprecise,
                byTICloseTimeNs);

        // call the AutoScaler and let it decide whether we scale primaries up/down
        try {
            //autoScaler.decideScaling(byTIId, reqCounter, imprecise);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void startByTI(long delayMs) {
        if(byTIStarted.compareAndSet(false, true)) {
            executorService.schedule(byTIClient, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Calculates the currently needed baseloadRate (or rather, the delay between baseloadRequests) in ms.
     *
     * @return Delay between two baseloadRequests in ms
     */
    public int calculateBClockIntervalMs() {
        int udsPrims = UDScheduler.getInstance().getCurrentUDSConfigurationPrimaries();
        // TODO check whether errors due to using integers in division are significant
        if(udsPrims == 1) {
            return maxRoundLatencyMs * byTIClient.getReplyQuorum();
        } else if(udsPrims > 1) {
            return maxRoundLatencyMs / ((udsPrims - 1) * byTIClient.getReplyQuorum());
        } else {
            // invalid number of udsPrims?!
            return maxRoundLatencyMs;
        }
    }

    public int getFirstReqInByTINo() {
        return firstReqInByTINo;
    }

    public int getLastReqInByTINo() {
        return lastReqInByTINo;
    }

    public boolean isImprecise() {
        return imprecise;
    }

    public long getByTICloseTimeNs() {
        return byTICloseTimeNs;
    }

    public int getByTIId() {
        return byTIId;
    }

    public int getLocalReqCounter() {
        return localReqCounter;
    }

    public int getDecidedReqCounter() {
        return decidedReqCounter;
    }

    public void resetReqCounter() {
        this.localReqCounter = 0;
    }

    public boolean isByTIStarted() {
        return byTIStarted.get();
    }

    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    public int getCurrentByTITickrateMs() {
        return currentByTITickrateMs;
    }
}
