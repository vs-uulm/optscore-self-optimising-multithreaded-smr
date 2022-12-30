package de.optscore.vscale.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import com.sun.management.UnixOperatingSystemMXBean;
import de.optscore.reconfiguration.cpu.CpuCore;
import de.optscore.reconfiguration.cpu.CpuReconfigurationException;
import de.optscore.reconfiguration.cpu.CpuReconfigurator;
import de.optscore.reconfiguration.cpu.LinuxCpuReconfigurator;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.server.byti.ByTIManager;
import de.optscore.vscale.util.BufferedStatsWriter;
import de.optscore.vscale.util.CSVWriter;
import de.optscore.vscale.util.EvalReqStatsServer;
import de.uniulm.vs.art.uds.DummySharedState;
import de.uniulm.vs.art.uds.UDSLock;
import de.uniulm.vs.art.uds.UDScheduler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Starting this via the main()-method spawns a (UDS)ServiceReplica (depending on arguments).
 * Provides all the code needed to handle client requests, i.e. create, lock and unlock mutexes, or simulate CPU load.
 */
public class EvalServer extends DefaultSingleRecoverable {

    private ServiceReplica serviceReplica;

    private ByTIManager byTIManager;

    /**
     * Holds DummyUDSharedStates which can be locked, modified and unlocked by EvalClients
     */
    private final List<DummySharedState> sharedStates;
    private final List<Lock> locks;
    private final boolean withUDS;

    public static final long BENCHMARK_NANOTIME_OFFSET = (System.currentTimeMillis() * 1000000) - System.nanoTime();

    // measured on lab-machines used for SRDS20-paper, a CPULoadNanos loop takes ~5000ns
    private static final long calibratedLoadDuration = 5000;

    /**
     * For adding and removing CPU cores at runtime
     */
    private CpuReconfigurator cpuReconfigurator;

    /**
     * Monitors current load by various means and reconfigures cpuCores according to its own strategies
     */
    private CpuReconfMonitor cpuReconfMonitor;

    /*
     * Stats logging stuff
     */
    public EvalReqStatsServer[][] runStats = null;
    private Map<Long, Double> cpuStats = null;

    private final UnixOperatingSystemMXBean statsBean;
    private final ExecutorService utilityPool = Executors.newCachedThreadPool();

    private BufferedStatsWriter byTIStatsWriter;
    private BufferedStatsWriter rawByTIStatsWriter;
    private BufferedStatsWriter evalReqStatsWriter;

    private final LinkedBlockingDeque<EvalReqStatsServer> evalReqStatsServerDeque;

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(EvalServer.class.getName());

    public EvalServer(int id, boolean withUDS, String testcaseId, int runNumber) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        // prepare output folders for raw data
        String replicaOutputPath = "eval-output-replicas/" + testcaseId + "/run" + runNumber + "/";
        if(new File(replicaOutputPath).mkdirs()) {
            logger.info("Created output directory for replica stats CSV files");
        } else {
            logger.finer("Could not create new directory for stats file dumps. Directory either already " +
                    "existed or permissions are incorrect.");
        }

        // CPU reconfiguration stuff
        try {
            //cpuStatsWriter = new BufferedStatsWriter(replicaOutputPath
            //        + "replicastats-cpuReconf-" + id + ".csv", new String[]{"currentTimeNs", "numOfActiveCores"});
            cpuReconfigurator = new LinuxCpuReconfigurator();
            //this.cpuReconfMonitor = new CpuReconfMonitor(cpuReconfigurator, cpuStatsWriter);
        } catch(CpuReconfigurationException e) {
            e.printStackTrace();
            logger.severe(e.getMessage());
            logger.severe("Could not load CPU reconfiguration, shutting down server!");
            System.exit(1);
        }

        int sharedStateCount = 32;
        int lockCount = 32;
        // create sharedStates EvalClients can use, which always means: lock the state's inherent UDSLock, add
        // modifying thread's name to state's internal StringList, unlock
        this.sharedStates = new ArrayList<>(sharedStateCount + 1);
        for(int i = 0; i < sharedStateCount; i++) {
            sharedStates.add(new DummySharedState(i, withUDS));
        }

        this.withUDS = withUDS;
        // create Locks EvalClients can lock/unlock however they want
        this.locks = new ArrayList<>(lockCount + 1);
        if(withUDS) {
            for(int i = 0; i < lockCount; i++) {
                locks.add(new UDSLock(i));
            }
        } else {
            for(int i = 0; i < lockCount; i++) {
                locks.add(new ReentrantLock());
            }
        }

        // for logging CPU stats
        this.statsBean = (UnixOperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean();

        // individual request profiling
        this.evalReqStatsServerDeque = new LinkedBlockingDeque<>();
        evalReqStatsWriter = new BufferedStatsWriter(replicaOutputPath
                + "replicastats-evalReqServer-" + id + ".csv",
                new String[]{"currentTimeNs", "reqReceivedInServiceReplica", "reqSubmittedtoUDS",
                        "reqStartedExecution", "reqEndedExecution", "reqFullyCompletedAndSentBackReply"});

        // TODO deal with recovery/snapshots, etc
        if(withUDS) {
            serviceReplica = new UDSServiceReplica(id, this, this);
        } else {
            serviceReplica = new ServiceReplica(id, this, this);
        }

        // ByTI stuff
        byTIStatsWriter = new BufferedStatsWriter(replicaOutputPath
                + "replicastats-byTI-" + id + ".csv",
                new String[]{"currentTimeNs", "byTIId", "firstNo", "lastNo", "reqCounter", "currentPrimaries",
                        "imprecise",
                        "byTICloseTime"});
        rawByTIStatsWriter = new BufferedStatsWriter(replicaOutputPath
                + "replicastats-rawByTI-" + id + ".csv",
                new String[]{"currentTimeNs", "senderId", "globalReqSequence", "tickReqCounter", "consensusId"});
        new Thread(() -> {
            try {
                Thread.sleep(10 * 1000);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
            // instantiate the byti manager (and with it a ByTIClient); processIds in the range of 8000+
            if(logger.isLoggable(Level.FINER)) {
                logger.finer("Creating ByTIManager ...");
            }
            byTIManager = new ByTIManager(8000 + id, byTIStatsWriter, rawByTIStatsWriter, false);

        }).start();
    }


    @Override
    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        // check whether there are any evalReqStats we can log to disk
        while(!evalReqStatsServerDeque.isEmpty()) {
            EvalReqStatsServer stats = evalReqStatsServerDeque.pollFirst();
            if(stats != null) {
                evalReqStatsWriter.writeEvalReqStatsServer(System.nanoTime() + BENCHMARK_NANOTIME_OFFSET, stats);
            } else {
                break;
            }
        }
        // TODO workaround to flush stuff to disk periodically before server is shutdown externally
        if(msgCtx.getGlobalReqSequence() % 200 == 0) {
            evalReqStatsWriter.flush();
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(command);
        DataInputStream dis = new DataInputStream(bis);

        try {
            // intCount = number of bytes / 4 in byte array
            // intCount / 2 is the number of commands (per command: 1 int for command, 1 int for parameter)
            int cmdCount = dis.available() / 4 / 2;

            // Rebuild and execute actions array from EvalClient
            StringBuilder actionLog = null;
            if(logger.isLoggable(Level.FINER)) {
                actionLog = new StringBuilder(Thread.currentThread().getName() + ": Received the following " +
                        "actions from Client " + msgCtx.getSender() + ": [ ");
            }

            int action;
            int parameter;
            boolean badRequest = false;
            byte[] reply = new byte[1];
            for(int i = 0; i < cmdCount; i++) {
                action = dis.readInt();
                parameter = dis.readInt();
                if(logger.isLoggable(Level.FINER)) {
                    actionLog.append(action).append(',').append(parameter);
                    if(i != cmdCount - 1) actionLog.append(" | ");
                }

                // extract the request type
                EvalActionType actionType = EvalActionType.values()[action];

                // handle the request
                switch(actionType) {
                    case LOCK:
                        // TODO temporary hack to start baseload when the test case starts
                        //  only works on test cases with locking requests (like LU1000LU)
                        // Start the byTIManagers with a slight delay depending on replicaID (phase shifted)
                        if(!byTIManager.isByTIStarted()) {
                            byTIManager.startByTI(serviceReplica.getId() * byTIManager.getCurrentByTITickrateMs() / 7);
                            logger.info("Received the first request with LOCK from " + msgCtx.getSender() + ". " +
                                    "Started ByTIManager.");
                        }

                        badRequest = lockDummyLock(parameter);
                        break;
                    case UNLOCK:
                        badRequest = unlockDummyUDSLock(parameter);
                        break;
                    case ADDTOSHAREDSTATE:
                        badRequest = addToSharedState(parameter);
                        break;
                    case SIMULATELOAD:
                        if(parameter > 0) {
                            simulateCPULoadNanos(parameter);
                        }
                        break;
                    case READONLY:
                        // do nothing
                        reply = new byte[]{0};
                        break;
                    case STATS_START:
                        // start CPU logging thread
                        cpuStats = new LinkedHashMap<>(500);
                        utilityPool.submit(() -> {
                            logger.finer("CPULogger starting ...");
                            double load;
                            long currentTime;
                            while(cpuStats != null) {
                                load = statsBean.getProcessCpuLoad();
                                currentTime = System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET;
                                logger.finest("CPULogger is saving (process-)CPU load of " + load + " at "
                                        + currentTime);
                                try {
                                    cpuStats.put(currentTime, load);
                                    Thread.sleep(250);
                                } catch(InterruptedException e) {
                                    // nothing happens except that we log more CPU load data points than planned
                                } catch(NullPointerException e) {
                                    // theoretically, there is a chance that cpuStats is cleaned inbetween a loop
                                    // start and the actual saving operation. In this case, just ignore the error and
                                    // stop the thread like it was supposed to
                                    logger.fine("CPULogger shutting down because of NullPointerException ...");
                                    break;
                                }
                            }
                            logger.finer("CPULogger shutting down ...");
                        });
                        break;
                    case STATS_DUMP:
                        // parameter = testcaseId
                        // prepare output folder for raw data
                        if(new File("eval-output-servers/test_case_" + parameter + "/").mkdirs()) {
                            logger.finest("Created output directory for raw client stats CSV files");
                        } else {
                            logger.warning("!!!! Could not create directory for stats file dumps. Please check " +
                                    "whether test case already existed and permissions are correct and maybe restart " +
                                    "testcase.");
                        }

                        logger.finer("Dumping cpuStats for testcase " + parameter);
                        // create new CSVWriter to dump stats to file for this run
                        CSVWriter cpuLogWriter = new CSVWriter(new File("eval-output-servers/test_case_" +
                                parameter + "/test_case_" + parameter + "-replica_" + serviceReplica.getId() +
                                "-cpuLoad.csv"));

                        // write header
                        cpuLogWriter.writeLine(Arrays.asList("nanoTime", "allCoresLoadAvg"));

                        // dump all logged CPU stats
                        for(long time : cpuStats.keySet()) {
                            cpuLogWriter.writeLine(Arrays.asList(Long.toString(time), cpuStats.get(time).toString()));
                        }
                        cpuLogWriter.flush();
                        cpuLogWriter.closeWriter();

                        // clean up all logged stats (timings + CPU load log). Unnecessary, but meh whatever
                        cpuStats.clear();
                        // setting cpuStats to null will stop CPU logging automatically
                        cpuStats = null;

                        break;
                    case REMOVE_CPU_CORES:
                        logger.warning("Deactivating " + parameter + " CPU core(s)!");
                        logger.finer("There are " + cpuReconfigurator.numberOfAvailableCpuCores() + "cores.");
                        logger.finer(cpuReconfigurator.numberOfActiveCpuCores() + " of those are active.");
                        if(logger.isLoggable(Level.FINEST)) {
                            try {
                                StringBuilder sb = new StringBuilder();
                                for(CpuCore cpuCore : cpuReconfigurator.listAvailableCores()) {
                                    sb.append(cpuCore.printStatus());
                                    sb.append("\n");
                                }
                                logger.finest(sb.toString());

                            } catch(CpuReconfigurationException e) {
                                e.printStackTrace();
                            }
                        }
                        // remove the cores
                        try {
                            this.cpuReconfigurator.removeCpuCores(parameter);
                        } catch(CpuReconfigurationException e) {
                            e.printStackTrace();
                        }
                        reply = new byte[1];
                        reply[0] = (byte) this.cpuReconfigurator.numberOfActiveCpuCores();
                        break;
                    case ADD_CPU_CORES:
                        logger.warning("Activating " + parameter + " CPU core(s)!");
                        try {
                            this.cpuReconfigurator.addCpuCores(parameter);
                        } catch(IndexOutOfBoundsException e) {
                            // we tried to activate a core that wasn't present ... which shouldn't matter
                            logger.warning("Got activation request " + parameter + " core(s), while only " +
                                    (cpuReconfigurator.numberOfAvailableCpuCores() -
                                    cpuReconfigurator.numberOfActiveCpuCores()) + " cores are available to " +
                                    "activate. Ignoring...");
                        }
                        reply = new byte[1];
                        reply[0] = (byte) this.cpuReconfigurator.numberOfActiveCpuCores();
                        break;
                    case RECONFIG_UDS_PRIMARIES:
                        logger.info("Reconfiguring UDS primaries to " + parameter + " ...");
                        reply = new byte[1];
                        final int newPrimaries = parameter;
                        Runnable reconfigPrimRunnable =
                                () -> UDScheduler.getInstance().requestReconfigurationPrimaries(newPrimaries);
                        UDScheduler.getInstance().addRequest(reconfigPrimRunnable, () -> {});
                        break;
                    case RECONFIG_UDS_STEPS:
                        logger.info("Reconfiguring UDS steps to " + parameter + " ...");
                        final int newSteps = parameter;
                        Runnable reconfigStepsRunnable =
                                () -> UDScheduler.getInstance().requestReconfigurationSteps(newSteps);
                        UDScheduler.getInstance().addRequest(reconfigStepsRunnable, () -> {});
                        break;
                    case ByTI:
                        if(logger.isLoggable(Level.FINEST)) {
                            logger.finest("Processing BaseloadRequest from replica " + msgCtx.getSender());
                        }
                        break;
                    default:
                        logger.severe(Thread.currentThread().getName() + ": Unrecognized EvalActionType " +
                                "received in request; type was: " + action);
                        return "400 Bad Request".getBytes();
                }
            }
            if(logger.isLoggable(Level.FINER)) {
                logger.finer(actionLog.append(" ]").toString());
            }

            if(!badRequest) {
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(Thread.currentThread().getName() + ": Successfully executed all actions from " +
                            "EvalClient " + msgCtx.getSender() + " for request with opId " + msgCtx.getOperationId() +
                            ". Returning reply.");
                }
                return reply;
            } else {
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(Thread.currentThread().getName() + ": EvalClient " + msgCtx.getSender() +
                            "'s request " + msgCtx.getOperationId() + " was malformed or included bad parameters");
                }
                return reply;
            }
        } catch(IOException e) {
            logger.severe("IOException while reading data from request in replica: " + e.getMessage());
            e.printStackTrace();
        } catch(CpuReconfigurationException e) {
            e.printStackTrace();
            logger.severe("Error while reconfiguring CPUs. Shutting down the server!");
            this.serviceReplica.kill();

            System.exit(1);
        } finally {
            if(withUDS) {
                msgCtx.getEvalReqStatsServer().setReqEndedExecution(System.nanoTime() + BENCHMARK_NANOTIME_OFFSET);
            }
        }
        return null;
    }

    private boolean addToSharedState(int sharedStateId) {
        if(sharedStateId >= sharedStates.size()) {
            // bad request
            return true;
        }

        DummySharedState state = sharedStates.get(sharedStateId);
        if(logger.isLoggable(Level.FINER)) {
            logger.finer(Thread.currentThread().getName() + " is modifying state in sharedState " + state.getId());
        }
        state.addToSharedState((Thread.currentThread().getName()));
        return false;
    }

    private boolean lockDummyLock(int lockId) {
        if(lockId >= locks.size()) {
            // bad request
            return true;
        }
        if(logger.isLoggable(Level.FINER)) {
            logger.finer(Thread.currentThread().getName() + ": Locking (UDS)Lock " + lockId);
        }
        locks.get(lockId).lock();
        return false;
    }

    private boolean unlockDummyUDSLock(int lockId) {
        if(lockId >= locks.size()) {
            // bad request
            return true;
        }

        // try to unlock the UDSLock. If the unlocking thread wasn't the owner of the mutex, a RuntimeException
        // (IllegalMonitorStateException) will be thrown
        if(logger.isLoggable(Level.FINER)) {
            logger.finer(Thread.currentThread().getName() + ": Unlocking (UDS)Lock " + lockId);
        }
        locks.get(lockId).unlock();
        return false;
    }

    private void printSharedStates() {
        for(DummySharedState state : sharedStates) {
            String stateState = state.getSharedState().stream().collect(Collectors.joining(", ",
                    "SharedState #" + state.getId() + ": [", "]"));
            stateState = (stateState.length() > 5000) ? stateState.substring(0, 5000) : stateState;
            logger.fine(stateState);
        }
    }

    private void resetStates() {
        int sharedStateCount = sharedStates.size();
        sharedStates.clear();
        for(int i = 0; i < sharedStateCount; i++) {
            sharedStates.add(new DummySharedState(i, withUDS));
        }
    }

    @Override
    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return new byte[0];
    }

    @Override
    public void installSnapshot(byte[] state) {
        // unused for our benchmarks
    }

    @Override
    public byte[] getSnapshot() {
        // unused for our benchmarks
        return new byte[0];
    }

    @Override
    public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
        return this.executeOrdered(command, msgCtx);
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        // Unused for our benchmarks
        return this.executeOrdered(command, msgCtx);
    }

    protected BufferedStatsWriter getByTIStatsWriter() {
        return byTIStatsWriter;
    }

    protected BufferedStatsWriter getRawByTIStatsWriter() {
        return rawByTIStatsWriter;
    }

    protected ByTIManager getByTIManager() {
        return byTIManager;
    }

    public LinkedBlockingDeque<EvalReqStatsServer> getEvalReqStatsServerDeque() {
        return evalReqStatsServerDeque;
    }

    /**
     * Fully occupies a thread for the given duration
     *
     * @param durationInMilliseconds how long the thread should spin for, in ms
     */
    private void simulateCPULoad(int durationInMilliseconds) {
        simulateCPULoad(durationInMilliseconds, 1.0d);
    }

    /**
     * Occupies the thread by simply spinning in a non-optimizable loop.
     * Optional parameter loadFactor specifies how much the thread should occupy a core if it would be scheduled to
     * run on a single core exclusively during the duration of the simulated load (ranging from 0-100%).
     * Adapted from https://caffinc.github.io/2016/03/cpu-load-generator/
     *
     * @param durationInMilliseconds how long the thread should spin for, in ms
     * @param loadFactor             how much the thread should work, ranging from 0 - 1.0 (0-100%); only takes effect when
     *                               duration is a lot longer than 100ms (~ >1000ms)
     */
    private void simulateCPULoad(int durationInMilliseconds, double loadFactor) {
        if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Simulating load with loadFactor " + loadFactor +
                    " for " + durationInMilliseconds + "ms");
        }
        long startTime = System.currentTimeMillis();
        try {
            // spin for given duration
            while(System.currentTimeMillis() - startTime < durationInMilliseconds) {
                // Every 100ms, sleep for percentage of unladen time
                if(System.currentTimeMillis() % 100 == 0) {
                    Thread.sleep((long) Math.floor((1 - loadFactor) * 100));
                }
            }
        } catch(InterruptedException e) {
            logger.finest(Thread.currentThread().getName() + ": Was interrupted " +
                    "while simulating CPU load (in Thread.sleep()).");
        }
        if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Finished simulating load");
        }
    }

    /**
     * Occupies the thread by spinning for a given number of nanoseconds (slightly
     * variable depending on the current systems JVM's accuracy).
     *
     * @param durationInNanoseconds How long the thread should spin for, in ns
     */
    private void simulateCPULoadNanos(int durationInNanoseconds) {
        /*if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Simulating load for " + durationInNanoseconds + "ns.");
        }
        long startTime = System.nanoTime();
        while(System.nanoTime() - startTime < durationInNanoseconds) ;
        if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Finished simulating load (" + durationInNanoseconds +
                    "ns)");
        }*/

        if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Simulating load for " + durationInNanoseconds + "ns.");
        }
        long e = 0;
        long s = -1000 * 1000 * 1000;
        long min = 1000 * 1000 * 1000;

        // simple self-calibrating CPU load: assumption is that Math.atan always needs
        // the same real-time span;
        // 150 iterations have been tested and need approx. 5000ns on the lab machines
        for (int i = 1; true; i++) {
            e = System.nanoTime();
            // the load
            for (int j = 0; j < 150; j++) {
                Math.atan((j % 100f) / 100f);
            }
            if ((e - s) < min)
                min = e - s;
            s = e;
            if (calibratedLoadDuration * i > durationInNanoseconds)
                break;
        }
        if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Finished simulating load (" + durationInNanoseconds +
                    "ns)");
        }
    }

    public static void main(String[] args) {
        if(args.length < 4) {
            printUsageAndExit();
        }

        try {
            int id = Integer.parseInt(args[0]);
            boolean withUDS = Boolean.parseBoolean(args[1]);
            String testcaseId = args[2];
            int runNumber = Integer.parseInt(args[3]);

            new EvalServer(id, withUDS, testcaseId, runNumber);
        } catch(NumberFormatException e) {
            printUsageAndExit();
        }
    }

    private static void printUsageAndExit() {
        System.out.println("===== Missing arguments");
        System.out.println("Usage: EvalServer <serverId> <withUDS> <testcaseId> <runNumber>");
        System.out.println("=====");
        System.exit(1);
    }
}
