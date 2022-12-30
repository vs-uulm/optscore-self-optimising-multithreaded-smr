package de.optscore.vscale.client;

import bftsmart.tom.ServiceProxy;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.coordination.PlaybookAction;
import de.optscore.vscale.coordination.TestcaseCoordinator;
import de.optscore.vscale.coordination.WorkloadPlaybook;
import de.optscore.vscale.util.BufferedStatsWriter;
import de.optscore.vscale.util.TestcaseSyncClient;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Main class of a clientMachine. Get's started by the coordinator, reads in a playbookFile, takes care of
 * clientGroups via a cgManager, starts/stops clients, logs stats and writes them to file so they can be pulled by
 * the TestCoordinator.
 */
public class ClientWorker implements Runnable {

    private static final Logger logger = Logger.getLogger(ClientWorker.class.getName());
    public static final Level GLOBAL_LOGGING_LEVEL = Level.INFO;

    public static final long BENCHMARK_NANOTIME_OFFSET = (System.currentTimeMillis() * 1000000) - System.nanoTime();

    private String testcaseId;
    private int runNumber;
    private int machineId;
    private String machineIP;

    private WorkloadPlaybook playbook;
    private ClientGroupManager clientGroupManager;

    private ExecutorService syncClientThreadPool = Executors.newFixedThreadPool(1);
    private TestcaseSyncClient syncClient;

    public ClientWorker(String coordinatorIP, int coordinatorPort, int machineId, String machineIP) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
        logger.info("Starting clientMachine ...");

        this.machineId = machineId;
        this.machineIP = machineIP;

        // Start syncClient thread for syncing with other clientWorkers
        this.syncClient = new TestcaseSyncClient(coordinatorIP, coordinatorPort);
        syncClientThreadPool.execute(syncClient);

        this.prepareTestcase();
    }

    /**
     * Reads in the playbookFile that was previously transferred to this clientMachine by the TestCoordinator.
     * Constructs a WorkloadPlaybook and instantiates ClientGroups (and thereby Clients).
     * Creates output folder for benchmark/logging data if it doesn't already exist.
     */
    private void prepareTestcase() {
        // read playbookFile to configure ourselves for the test case
        File tmpDir = new File("tmp/");
        File playbookFile = new File(tmpDir, "playbook_machine-" + machineId + "-" + machineIP + ".plb");
        try {
            BufferedReader playbookReader = new BufferedReader(new FileReader(playbookFile));

            // get testcaseId, minPid, maxPid and create playbook
            Map<String, String> metaInformation = new HashMap<>(5);

            for(String kvPair : playbookReader.lines().findFirst().get().split("\\|")) {
                String[] tmp = kvPair.split("\\$");
                metaInformation.put(tmp[0], tmp[1]);
            }
            int minPid = Integer.parseInt(metaInformation.get("minPid"));
            int maxPid = Integer.parseInt(metaInformation.get("maxPid"));
            this.testcaseId = metaInformation.get("testcaseId");
            this.runNumber = Integer.parseInt(metaInformation.get("runNumber"));

            // check whether we are the machine responsible for replica CPU cores and UDS config (signalled by
            // numOfCoresToActivate > 0)
            int numOfCoresToActivate = Integer.parseInt(metaInformation.get("numOfCoresToActivate"));
            int udsPrims = Integer.parseInt(metaInformation.get("udsPrims"));
            int udsSteps = Integer.parseInt(metaInformation.get("udsSteps"));
            if(numOfCoresToActivate > 0) {
                logger.warning("##### Reconfiguring cores and UDS  on replicas...");
                // we are the machine that has to activate cores. Instantiate a temp client and make sure it is "active"
                ServiceProxy cpuClient = new ServiceProxy(5000);
                cpuClient.invokeOrdered(EvalRequest.serializeEvalRequest(new EvalRequest.EvalRequestBuilder()
                                .action(EvalActionType.READONLY.getActionTypeCode(), 0).build()));

                // first remove all CPU cores on the replicas (except the first one, which can never be deactivated)
                logger.warning("Removing " + TestcaseCoordinator.MAX_CPU_CORES + " cores ...");
                EvalRequest removeAllCoresReq = new EvalRequest.EvalRequestBuilder()
                        .action(EvalActionType.REMOVE_CPU_CORES.getActionTypeCode(), TestcaseCoordinator.MAX_CPU_CORES)
                        .build();
                cpuClient.invokeOrdered(EvalRequest.serializeEvalRequest(removeAllCoresReq));

                // sleep 2 secs to make sure everything is done and only one core is left active on the replicas
                // the other clients will wait until we're done
                threadSleep(2 * 1000);

                logger.warning("Adding " + (numOfCoresToActivate - 1) + " cores ...");
                // then activate the correct number of cores for this test (-1 because the first core is already active)
                EvalRequest addCoresReq = new EvalRequest.EvalRequestBuilder()
                        .action(EvalActionType.ADD_CPU_CORES.getActionTypeCode(), numOfCoresToActivate - 1)
                        .build();
                cpuClient.invokeOrdered(EvalRequest.serializeEvalRequest(addCoresReq));

                // sleep another 2 seconds
                threadSleep(2 * 1000);

                if(udsPrims > 0) {
                    logger.warning("Reconfiguring UDS to " + udsPrims + " primaries ...");
                    EvalRequest udsPrimReq = new EvalRequest.EvalRequestBuilder()
                            .action(EvalActionType.RECONFIG_UDS_PRIMARIES.getActionTypeCode(), udsPrims)
                            .build();
                    cpuClient.invokeOrdered(EvalRequest.serializeEvalRequest(udsPrimReq));
                    threadSleep(1000);
                }

                if(udsSteps > 0) {
                    logger.warning("Reconfiguring UDS to " + udsSteps + " steps ...");
                    EvalRequest udsStepsReq = new EvalRequest.EvalRequestBuilder()
                            .action(EvalActionType.RECONFIG_UDS_STEPS.getActionTypeCode(), udsSteps)
                            .build();
                    cpuClient.invokeOrdered(EvalRequest.serializeEvalRequest(udsStepsReq));
                    threadSleep(1000);
                }

                // dispose of the temporary client
                cpuClient.close();
                logger.warning("Finished reconfiguring cores and UDS! Proceeding as usual ...");
            }

            this.playbook = new WorkloadPlaybook(testcaseId);

            // prepare output folders for raw data
            String clientOutputPath = "eval-output-clients/" + testcaseId + "/run" + runNumber + "/";
            if(new File(clientOutputPath).mkdirs()) {
                logger.info("Created output directory for raw client stats CSV files");
            } else {
                logger.finer("Could not create new directory for stats file dumps. Directory either already " +
                        "existed or permissions are incorrect.");
            }

            // create a ClientGroupManager, which takes care of ClientGroups, with a BufferedWriter for stats logging
            this.clientGroupManager = new ClientGroupManager(minPid, maxPid,
                    new BufferedStatsWriter(clientOutputPath
                            + "clientstats-" + machineId + "-" + machineIP + ".csv",
                            new String[]{"clientPid", "opId", "sentTimeNs", "receivedTimeNs"}));

            // get all playbookActions for this machine and add them to a playbook
            playbookReader.lines()
                    .map(PlaybookAction::createPlaybookActionFromString)
                    .forEach(playbookAction -> playbook.addPlaybookAction(playbookAction));

            Set<Integer> clientGroupIds = playbook.getPlaybookActions().stream()
                    .map(action -> action.getClientGroup().getId())
                    .collect(Collectors.toSet());

            Map<Integer, Integer> grpCnt = playbook.getActiveClientGroupsCountByIdForMachine(machineId);
            for(int i : clientGroupIds) {
                clientGroupManager.createAndAddClientGroupInstancesForGroupId(i,
                        grpCnt.get(i),
                        playbook.getClientGroupNumOfClients(i),
                        playbook.getClientGroupRequestProfile(i),
                        playbook.getClientGroupSendDelay(i));
            }
            // after clients have been instantiated, wait for a few secs until all initial workaround requests (see
            // comment in SynchronousEvalClient constructor) have gone through and channels are really active
            threadSleep(2000);

            //remove tmpDir with the playbookFile
            playbookFile.delete();
            tmpDir.delete();

        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        // wait here until all clientWorkers are ready
        logger.info("Waiting until all clientMachines have prepared their test case ...");
        syncClient.waitForAllClientsReady();
        logger.info("All clientMachines ready, starting playbook replay ...");

        // then start the main clientMachine loop, which will sleep until the next action is due,
        // execute all actions that have to be executed at the current time, sleep again, etc.
        long currentTimeNs = System.nanoTime() + BENCHMARK_NANOTIME_OFFSET;
        long playbookStartTimeNs = currentTimeNs;
        long currentTimeOffsetNs = 0L;
        logger.finer("Playbook is " + (playbook.getPlaybookDuration() / 1000000) + "ms long.");

        while(currentTimeOffsetNs < playbook.getPlaybookDuration()) {
            // update current time in ns
            currentTimeNs = System.nanoTime() + BENCHMARK_NANOTIME_OFFSET;
            currentTimeOffsetNs = currentTimeNs - playbookStartTimeNs;

            // 1. get all playbook actions which have a timeOffsetNs smaller than the currentTimeOffsetNs
            logger.finest("Looking for actions that should be executed at current time ("
                    + currentTimeOffsetNs / 1000000 + "ms).");
            PlaybookAction[] executableActions = playbook.getExecutableActions(currentTimeOffsetNs);

            // 2. execute those actions and remove them from the playbook
            logger.finest("Executing " + executableActions.length + " actions ...");
            for(PlaybookAction action : executableActions) {
                executePlaybookAction(action);
                this.playbook.removeActionFromPlaybook(action);
            }

            // 3. get the next playbook action and sleep until close to the timeOffsetNs of that action
            Optional<PlaybookAction> nextActionOptional = playbook.getNextAction(currentTimeOffsetNs);
            long sleepTimeMs;
            if(nextActionOptional.isPresent()) {
                 sleepTimeMs = (nextActionOptional.get().getTimeOffsetNs() - currentTimeOffsetNs) / 1000000;
            } else {
                // no next actions means we have executed the entire playbook. End.
                break;
            }

            // 4. sleep until approx. the next action. Either the thread wakes up too late, in which case the action(s)
            // will immediately be performed, or the thread wakes up too early (is this possible?) and the loop will
            // continue until currentTimeOffsetNs is finally bigger than the next action's timeOffsetNs
            logger.finest("Sleeping for " + sleepTimeMs + "ms, until the next action.");
            threadSleep(sleepTimeMs);
        }

        // wait for a bit until all clients have dealt with their last actions (should normally be put to sleep ...)
        threadSleep(2000);

        // shutdown syncClient so the server knows this clientMachine is done with its playbook
        logger.info("Completed playbook replay, shutting down clientMachine ...");
        clientGroupManager.closeAllGroups();
        

        try {
            syncClient.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
        syncClientThreadPool.shutdown();

        logger.info("##### Shutting down ClientWorker ...");
        // the brutal method, because sometimes some threads seem to linger ... got fed up, sorry.
        System.exit(0);
    }

    private void executePlaybookAction(PlaybookAction action) {
        logger.fine("Executing a PlaybookAction: " + action.toString());
        clientGroupManager.modifyClientGroupStates(action.getClientGroup().getId(), action.getAddRemoveModifier());
    }

    /**
     * convenience method
     * @param millis for how long the calling thread should sleep
     */
    private static void threadSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if(args.length < 4) {
            System.out.println("Client needs the following arguments to start: coordinatorIP coordinatorPort " +
                    "machineId machineIP");
            System.exit(2);
        }

        ClientWorker worker = new ClientWorker(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
        new Thread(worker).start();

    }

}
