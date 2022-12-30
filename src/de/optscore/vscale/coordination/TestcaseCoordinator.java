package de.optscore.vscale.coordination;

import com.jcabi.ssh.Ssh;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * @author Gerhard Habiger, Dominik Meißner
 */
public class TestcaseCoordinator {

    public static final String DEFAULT_TEST_COORDINATOR_HOST = "127.0.0.1";
    public static final int DEFAULT_TEST_COORDINATOR_PORT = 13337;

    private static final String sshUsername = "[username]";
    private static final String sshKeyfilePath = "/home/" + sshUsername + "/.ssh/id_rsa";
    private static final String debugDir = "debug-output/";
    private static final int sshPort = 22;

    // the maximum number of CPU cores available in replicas. Change according to current test hardware.
    public static final int MAX_CPU_CORES = 8;

    private String testCoordinatorIP;
    private int testCoordinatorPort;

    private DMarkAdapter dMarkAdapter;

    private Testcase testcase;

    private Map<Integer, Ssh> replicaSsh;
    private Map<Integer, Ssh> clientSsh;

    private ServerSocket serverSocket;
    private ExecutorService threadPool;

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(TestcaseCoordinator.class.getName());

    public TestcaseCoordinator(String dbFilePath) throws IOException {
        this(dbFilePath, DEFAULT_TEST_COORDINATOR_HOST, DEFAULT_TEST_COORDINATOR_PORT, "test");
    }

    public TestcaseCoordinator(String dbPath, String coordinatorIP, int coordinatorPort, String testcaseId) throws
            IOException {
        logger.setLevel(Level.FINEST);

        this.dMarkAdapter = new DMarkAdapter(dbPath);
        this.testCoordinatorIP = coordinatorIP;
        this.testCoordinatorPort = coordinatorPort;
        this.testcase = dMarkAdapter.objectifyTestcase(testcaseId);

        serverSocket = new ServerSocket(coordinatorPort);
        serverSocket.setSoTimeout(0);
        threadPool = Executors.newCachedThreadPool();

        logger.finer("TestcaseCoordinator running ...");
    }

    /**
     * Runs an entire test case.
     * 1. Creates tmp dir for playbookFiles. Creates playbookFiles for each clientMachine in the test case.
     * 2. Connects to clientMachines and replicas
     * 3. Transfers playbookFiles to clientMachines
     * 4. Starts replicas, waits a few seconds until they have connected to each other
     * 5. Starts clients
     * 6. Waits for clients to connect (via SynClients) so we know when the test case has finished
     * 7. Cleans up by killing all JVMs on clientMachines and replicas
     * 8. Transfers created stats logging files on clientMachines and replicas
     */
    public void runTestcase() {
        TestcaseConfiguration testcaseConfiguration = testcase.getTestcaseConfiguration();
        WorkloadPlaybook playbook = testcase.getWorkloadPlaybook();

        // create tmp dir for playbook files
        File tmpDir = new File("tmp/");
        tmpDir.mkdir();

        // create the individual playbook files for the client machines
        Integer[] clientMachineIds = playbook.getClientMachineIds();
        for(int i : clientMachineIds) {
            // tell the first machine to configure cores on the replicas
            int numOfCoresToActivate = ((i == clientMachineIds[0]) ? testcaseConfiguration.numOfActiveCores : -1);
            createPlaybookFile(playbook, i, testcaseConfiguration, numOfCoresToActivate);
        }

        // start main testcase coordination
        logger.info("\n\n##### Starting Testcase "
                + testcaseConfiguration.testcaseId
                + " (\"" + testcaseConfiguration.testcaseDesc
                + "\") in run #" + testcaseConfiguration.runsCompleted);

        try {
            // Connect to all replicas and clientWorkers
            logger.info("Connecting to replicas and clientMachines ...");
            replicaSsh = new HashMap<>(testcaseConfiguration.numOfReplicas + 1);
            clientSsh = new HashMap<>(clientMachineIds.length + 1);

            for(int j = 0; j < testcaseConfiguration.numOfReplicas; j++) {
                replicaSsh.put(j, sshLogin(testcaseConfiguration.replicaIPs[j], sshPort, sshUsername, sshKeyfilePath));
            }
            for(int j : clientMachineIds) {
                clientSsh.put(j, sshLogin(playbook.getClientMachineIp(j), sshPort, sshUsername,
                        sshKeyfilePath));
            }

            // if all connections have been established successfully, continue; also create output streams for
            // debug files for both replicas and clientWorkers
            boolean connectionsSuccessful = true;
            Map<Integer, OutputStream> replicaFos = new HashMap<>(5);
            Map<Integer, OutputStream> clientWorkerFos = new HashMap<>(5);
            for(int j : replicaSsh.keySet()) {
                connectionsSuccessful &= (replicaSsh.get(j) != null);
                // only log when the LogLevel is finer than WARNING
                if(logger.getLevel().intValue() < Level.WARNING.intValue()) {
                    replicaFos.put(j, new FileOutputStream(new File(debugDir + "replicas/" +
                            testcaseConfiguration.testcaseId + "-run" + testcaseConfiguration.runsCompleted +
                            "-replica" + j + ".out")));
                } else {
                    // null logging which may perhaps improve performance. No idea.
                    replicaFos.put(j, new OutputStream() {
                        @Override
                        public void write(int b) throws IOException {
                        }
                    });
                }
            }
            for(int j : clientSsh.keySet()) {
                connectionsSuccessful &= (clientSsh.get(j) != null);
                // only log when the LogLevel is finer than WARNING
                if(logger.getLevel().intValue() < Level.WARNING.intValue()) {
                    clientWorkerFos.put(j, new FileOutputStream(new File(debugDir + "clientWorkers/" +
                            testcaseConfiguration.testcaseId + "-run" + testcaseConfiguration.runsCompleted +
                            "-clientWorker" + j + ".out")));
                } else {
                    // null logging which may perhaps improve performance. No idea.
                    replicaFos.put(j, new OutputStream() {
                        @Override
                        public void write(int b) throws IOException {
                        }
                    });
                }
            }

            if(connectionsSuccessful) {
                logger.finer("All shells created, connections to servers and clients active");


                // transfer playbook files to clientMachines
                for(int i : clientMachineIds) {
                    String clientMachineIP = playbook.getClientMachineIp(i);
                    distributePlaybookFile(new File(getPlaybookFileName(i, clientMachineIP)), clientMachineIP);
                }


                // start BFT-SMaRt replicas
                logger.finer("Starting replicas ...");
                for(int i : replicaFos.keySet()) {
                    startReplica(i, testcaseConfiguration, replicaFos.get(i));
                }

                // wait 15s until replicas have connected themselves and are ready for requests
                    try {
                        Thread.sleep(15 * 1000);
                    } catch(InterruptedException e) {
                    // hopefully ain't happenin', and if so we're shit out of luck
                    logger.severe("Testcasecoordinator was interrupted while waiting for replicas to connect." +
                            " Test might be corrupt if we continue. Please manually check replicas and " +
                            "clients and kill JVMs if necessary. Aborting ...");
                    System.exit(10);
                }

                // replicas should be up and reconfigured, so start clients
                logger.finer("Starting clientWorkers ...");
                for(int i : clientMachineIds) {
                    startClientMachine(i, playbook.getClientMachineIp(i), clientWorkerFos.get(i));
                }


                // TODO check whether some of this stuff can be moved to methods
                // now we open server sockets and wait for clients to finish their work ...
                int numOfClientsConnected = 0;
                // initialize barrier for SyncClient waiting stuff
                final CyclicBarrier barrier = new CyclicBarrier(clientMachineIds.length);
                final CyclicBarrier allClientsDone = new CyclicBarrier(clientMachineIds.length + 1);
                while(numOfClientsConnected < clientMachineIds.length) {
                    try {
                        // wait for a syncClient to connect
                        Socket socket = serverSocket.accept();
                        numOfClientsConnected++;
                        // start a new thread to handle the syncClient
                        threadPool.execute(() -> {
                            logger.finer("SyncClient connected " + socket.getInetAddress());
                            try {
                                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

                                for(String line; (line = reader.readLine()) != null; ) {
                                    logger.finest(line);
                                    String[] parts = line.split("-", 2);
                                    try {
                                        if(parts.length == 2 && parts[0].equals("ready")) {
                                            logger.finer("Info: received ready from " + socket.toString());
                                            // wait on the barrier until all other syncClients are ready
                                            barrier.await();
                                            // all syncClients have sent ready-messages, tell every client to proceed
                                            writer.println("go-" + parts[1]);
                                            logger.finer("Info: sending go to " + socket.toString());
                                        } else if(parts.length == 2 && parts[0].equals("close")) {
                                            break;
                                        } else {
                                            logger.warning("Warning: received unknown message type");
                                        }
                                    } catch(InterruptedException | BrokenBarrierException e) {
                                        // in case the barrier breaks, send a reset to the clients
                                        writer.println("reset-" + parts[1]);
                                        logger.warning("Warning: barrier broke, resetting clients");
                                    }
                                }
                            } catch(IOException e) {
                                e.printStackTrace();
                            }

                            // wait until all clients are finished with all testruns, then terminate
                            try {
                                logger.fine("Waiting on all Clients done barrier");
                                allClientsDone.await();
                                logger.fine("All Clients done barrier reached");
                            } catch(InterruptedException | BrokenBarrierException e1) {
                                e1.printStackTrace();
                            }
                        });
                    } catch(IOException e) {
                        e.printStackTrace();
                    }
                }
                logger.finer("All SyncClients connected. Waiting until test case is done ...");

                // wait until testcase is completed
                try {
                    allClientsDone.await();
                    logger.info("##### Testcase done, killing all replica JVMs in 5 seconds! (Clients have to " +
                            "terminate themselves, in case a TestCoordinator is running on a client machine)");

                    // in the meantime, clean up, delete playbookfiles and tmp dir
                    for(int i : clientMachineIds) {
                        logger.finest("Cleaning up tmp playbookFiles");
                        String clientMachineIP = playbook.getClientMachineIp(i);
                        new File(getPlaybookFileName(i, clientMachineIP)).delete();
                    }
                    tmpDir.delete();

                    // wait 5 secs until CPU stats are dumped and hanging requests are done / queues are emptied
                    threadSleep(5 * 1000);

                    // kill all replica JVMs; for safety, in case anything didn't terminate properly
                    replicaSsh.keySet().forEach(
                            j -> threadPool.execute(() -> {
                                try {
                                    replicaSsh.get(j).exec("sudo killall -15 java",
                                            null,
                                            System.out,
                                            System.out
                                    );
                                } catch(IOException e) {
                                    e.printStackTrace();
                                }
                            })
                    );

                    logger.info("Transferring all created output files to TestCoordinator host in 3 seconds...");
                    threadSleep(3 * 1000);

                    // transfer all created output files from cluster to TestCoordinator
                    ProcessBuilder rsyncBuilder;
                    String evalOutputPath = "~/optscore-self-optimising-multithreaded-smr/eval-output";
                    for(int j : clientMachineIds) {
                        String clientMachineIP = playbook.getClientMachineIp(j);
                        // transfer client request stats
                        rsyncBuilder = new ProcessBuilder();
                        rsyncBuilder.inheritIO();
                        try {
                            rsyncBuilder.command("rsync",
                                    "-e",
                                    "ssh -i " + sshKeyfilePath,
                                    "-avzhR",
                                    // remote path
                                    sshUsername + "@" + clientMachineIP + ":" + evalOutputPath
                                            + "-clients/./" + testcaseConfiguration.testcaseId
                                            + "/run" + testcaseConfiguration.runsCompleted + "/*",
                                    // local path
                                    "eval-output/");
                            logger.finest("Rsync command: " + rsyncBuilder.command().stream().collect(Collectors.joining(" ")));
                            rsyncBuilder.start().waitFor();
                        } catch(IOException e) {
                            e.printStackTrace();
                        }
                    }
                    for(int j = 0; j < testcaseConfiguration.replicaIPs.length; j++) {
                        rsyncBuilder = new ProcessBuilder();
                        rsyncBuilder.inheritIO();
                        try {
                            rsyncBuilder.command("rsync",
                                    "-e",
                                    "ssh -i " + sshKeyfilePath,
                                    "-avzhR",
                                    // remote path
                                    sshUsername + "@" + testcaseConfiguration.replicaIPs[j] + ":" + evalOutputPath
                                            + "-replicas/./" + testcaseConfiguration.testcaseId
                                            + "/run" + testcaseConfiguration.runsCompleted + "/*",
                                    // local path
                                    "eval-output/");
                            logger.finest("Rsync command: " + rsyncBuilder.command().stream().collect(Collectors.joining(" ")));
                            rsyncBuilder.start().waitFor();
                        } catch(IOException e) {
                            e.printStackTrace();
                        }
                    }

                    // mark the test case run as completed in the database
                    dMarkAdapter.incrementTestcaseRunNumber(testcaseConfiguration.testcaseId);

                    // and create a documentation file with all relevant test case parameters in the outputfolder
                    createTestcaseDocumentationFile(testcase);

                    // wait 2 secs, just for safety and termination of everything. Probably unnecessary
                    Thread.sleep(2 * 1000);
                } catch(InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            logger.finer("Disposing of DB connection and SyncClient socket & threadPool...");
            dMarkAdapter.closeConnection();
            serverSocket.close();
            threadPool.shutdown();
        } catch(IOException e) {
            e.printStackTrace();
        }
        logger.info("\n##### Test case " + testcaseConfiguration.testcaseId + " (run #" +
                testcaseConfiguration.runsCompleted + ") completed. Shutting down TestcaseCoordinator...");
    }

    private void createTestcaseDocumentationFile(Testcase testcase) {



        // FIXME somehow, the testcaseParameters.txt contain two different testcaseIds at the moment. Investigate & fix



        File testcaseDoc = new File("eval-output/" + testcase.getTestcaseConfiguration().testcaseId
                + "/run" + testcase.getTestcaseConfiguration().runsCompleted + "/testcaseParameters.txt");
        try(FileOutputStream testcaseDocFos = new FileOutputStream(testcaseDoc)) {
            testcaseDocFos.write(testcase.getTestcaseSummary().getBytes(StandardCharsets.UTF_8));
            testcaseDocFos.flush();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a new playbookFile for a single clientMachine, which can then be transferred to that machine
     * @param machineId The Id of the machine this playbook should be for
     */
    private void createPlaybookFile(WorkloadPlaybook playbook,
                                    int machineId,
                                    TestcaseConfiguration config,
                                    int numOfCoresToActivate) {
        File playbookFile = new File(getPlaybookFileName(machineId, playbook.getClientMachineIp(machineId)));

        try(FileOutputStream playbookFos = new FileOutputStream(playbookFile)) {
            playbookFos.write(playbook
                    .serializePlaybookForMachine(machineId,
                            config.runsCompleted,
                            numOfCoresToActivate,
                            config.udsConfPrim,
                            config.udsConfSteps)
                    .getBytes(StandardCharsets.UTF_8));
            playbookFos.flush();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Transfers a playbookFile to a clientMachine with scp and then deletes it locally
     * @param playbookFile The file that is to be transferred
     * @param clientMachineIP The IP of the clientMachine that will receive the file via scp
     */
    private void distributePlaybookFile(File playbookFile, String clientMachineIP) {
        logger.finer("Transferring playbookFile " + playbookFile.getPath() + " to clientMachine " + clientMachineIP);
        ProcessBuilder rsyncBuilder = new ProcessBuilder().inheritIO();
        try {
            rsyncBuilder.command("rsync",
                    "-e",
                    "ssh -i " + sshKeyfilePath,
                    "-vzh",
                    playbookFile.getPath(),
                    sshUsername + "@" + clientMachineIP
                            + ":/home/" + sshUsername + "/optscore-self-optimising-multithreaded-smr/tmp/");
            rsyncBuilder.start().waitFor();
        } catch(IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void startReplica(int replicaId, TestcaseConfiguration testcaseConfiguration, OutputStream logOutputStream) {
        logger.finer("Starting BFT-SMaRt replica " + replicaId + " ...");
        threadPool.execute(() -> {
            try {
                replicaSsh.get(replicaId).exec(startBftReplicaCommand(replicaId, testcaseConfiguration.withUDS,
                        testcaseConfiguration.testcaseId, testcaseConfiguration.runsCompleted),
                        null,
                        logOutputStream,
                        logOutputStream
                );
            } catch(IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void startClientMachine(int clientMachineId, String clientMachineIP, OutputStream logOutputStream) {
        logger.finer("Starting clientMachine " + clientMachineId + " ...");
        threadPool.execute(() -> {
            try {
                clientSsh.get(clientMachineId).exec(startClientCommand(clientMachineId, clientMachineIP),
                        null,
                        logOutputStream,
                        logOutputStream
                );
            } catch(IOException e) {
                e.printStackTrace();
            }
        });
    }

    private Ssh sshLogin(String hostname, int port, String username, String keyfilePath) {
        logger.finest("Using keyfile " + keyfilePath + " for SSH to " + username + "@" + hostname + ":" + port);
        File keyFile = new File(keyfilePath);
        try {
            BufferedReader kfReader = new BufferedReader(new InputStreamReader(new FileInputStream(keyFile)));
            String privKey = kfReader.lines().collect(Collectors.joining("\n"));
            kfReader.close();
            return new Ssh(hostname, port, username, privKey);
        } catch(IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String getPlaybookFileName(int clientMachineId, String clientMachineIP) {
        return "tmp/playbook_machine-"
                + clientMachineId
                + "-"
                + clientMachineIP
                + ".plb";
    }

    private String startBftReplicaCommand(int processId, boolean withUDS, String testcaseId, int runNumber) {
        return "cd ~/optscore-self-optimising-multithreaded-smr && sudo java -server -Xms1g -Xmx16g " +
                "-Djava.util.logging.config.file=logging.properties " +
                "-Dlogback.configurationFile=./config/logback.xml " +
                "-cp lib/*:out/production/optscore-self-optimising-multithreaded-smr de.optscore.vscale.server.EvalServer "
                + processId + " "
                + withUDS + " "
                + testcaseId + " "
                + runNumber;
    }

    private String startClientCommand(int clientMachineId, String clientMachineIP) {
        return "cd ~/optscore-self-optimising-multithreaded-smr && "
                + "java -Djava.util.logging.config.file=logging.properties "
                + "-Dlogback.configurationFile=./config/logback.xml "
                + "-cp lib/*:out/production/optscore-self-optimising-multithreaded-smr "
                + "de.optscore.vscale.client.ClientWorker "
                + testCoordinatorIP + " "
                + testCoordinatorPort + " "
                + clientMachineId + " "
                + clientMachineIP;
    }

    private static void threadSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Please specify a test case DB file path.");
            System.exit(1);
        }
        String dbFilePath = args[0];
        List<String> testcases = new LinkedList<>();
            if(args.length >= 4) {
                String coordinatorIP = args[1];
                int coordinatorPort = Integer.parseInt(args[2]);
                for(int i = 3; i < args.length; i++) {
                    testcases.add(args[i]);
                }

                for(String testcaseId : testcases) {
                    try {
                        TestcaseCoordinator coordinator = new TestcaseCoordinator(dbFilePath, coordinatorIP,
                                coordinatorPort, testcaseId);
                        coordinator.runTestcase();
                        // wait 2 seconds inbetween test cases
                        threadSleep(2000);
                    } catch(IOException e) {
                        e.printStackTrace();
                        System.err.println("ERROR: Could not instantiate TestCoordinator for testcase " + testcaseId);
                    }
                }

                // just kill this abomination, as it often won't die silently
                System.exit(0);
            } else {
                System.out.println("Please specify a test case DB file path, hostname, port and test case ID(s) for " +
                        "the TestcaseCoordinator");
                System.exit(2);
            }
    }
}
