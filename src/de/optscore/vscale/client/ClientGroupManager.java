package de.optscore.vscale.client;

import com.lmax.disruptor.dsl.Disruptor;
import de.optscore.vscale.RequestProfile;
import de.optscore.vscale.RequestProfileRepository;
import de.optscore.vscale.util.BufferedStatsWriter;
import de.optscore.vscale.util.EvalReqStatsClient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Manages {@link ClientGroupInstance}s, meaning this class creates groups, tells them to create clients, and starts/stops
 * groups' clients whenever a testaction commands it.
 * Provides a range of pids for this clientMachine for instantiating clients. The range is supplied by the test
 * coordinator.
 */
public class ClientGroupManager {
    private static final Logger logger = Logger.getLogger(ClientGroupManager.class.getName());

    private int maxPid;
    private int currentPid;

    private Map<Integer, ClientGroup[]> clientGroupsMapping;

    private RequestProfileRepository requestProfileRepository;

    private BufferedStatsWriter statsWriter;
    private Disruptor<EvalReqStatsClient> loggingDisruptor;


    public ClientGroupManager(int minPid, int maxPid, BufferedStatsWriter statsWriter) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.currentPid = minPid - 1;
        this.maxPid = maxPid;

        this.clientGroupsMapping = new HashMap<>(5);

        this.requestProfileRepository = new RequestProfileRepository();

        this.statsWriter = statsWriter;
        logger.finest("Firing up loggingDisruptor for sequencing SyncEvalClients' reqStats output to disk ...");
        //noinspection deprecation
        this.loggingDisruptor = new Disruptor<>(EvalReqStatsClient::new,
                1024,
                Executors.newFixedThreadPool(1));
        loggingDisruptor.handleEventsWith((event, sequence, endOfBatch) -> statsWriter.writeEvalReqStatsClient(event));
        loggingDisruptor.start();
    }

    public void createAndAddClientGroupInstancesForGroupId(int clientGroupId,
                                                           int numOfClientGroups,
                                                           int numOfClients,
                                                           RequestProfile requestProfile,
                                                           long sendDelayNs) {
        ClientGroup[] clientGroups = new ClientGroup[numOfClientGroups];
        for(int i = 0; i < numOfClientGroups; i++) {
            clientGroups[i] = new ClientGroupInstance(clientGroupId,
                    numOfClients,
                    requestProfile,
                    sendDelayNs,
                    this,
                    loggingDisruptor);
        }
        clientGroupsMapping.put(clientGroupId, clientGroups);
    }

    /**
     * Activates/deactivates a number of specific clientGroup(s)
     * @param clientGroupId The kind of clientGroup(s) that should be de-/activated
     * @param addRemoveModifier How many client groups to de-/activate. Positiv for activation, negative otherwise
     */
    public void modifyClientGroupStates(int clientGroupId, int addRemoveModifier) {
        if(addRemoveModifier > 0) {
            Arrays.stream(clientGroupsMapping.get(clientGroupId))
                    .filter(clientGroup -> !clientGroup.isActive())
                    .limit(addRemoveModifier).forEach(ClientGroup::activate);
        } else if(addRemoveModifier < 0) {
            Arrays.stream(clientGroupsMapping.get(clientGroupId))
                    .filter(ClientGroup::isActive)
                    .limit(Math.abs(addRemoveModifier)).forEach(ClientGroup::deactivate);
        }
        // if addRemoveModifier is 0, we don't do anything
    }

    public RequestProfileRepository getRequestProfileRepository() {
        return requestProfileRepository;
    }

    public int getNextPid() throws IndexOutOfBoundsException {
        if((currentPid + 1) <= maxPid) {
            return ++currentPid;
        } else {
            throw new IndexOutOfBoundsException("Tried to getNextPid with currentPid = " + currentPid
                    + " and maxPid = " + maxPid);
        }
    }

    public void closeAllGroups() {
        for(ClientGroup[] groups : clientGroupsMapping.values()) {
            for(ClientGroup group : groups) {
                group.shutdown();
            }
        }
        loggingDisruptor.shutdown();
        // also close the statsWriter, causing it to flush everything that's still buffered to disk
        this.statsWriter.close();
    }
}
