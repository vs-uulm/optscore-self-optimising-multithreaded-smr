package de.uniulm.vs.art.uds;

import de.optscore.vscale.client.ClientWorker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class UDSTester {

    private static final Logger logger = Logger.getLogger(UDSTester.class.getName());

    private final int numberOfThreadsToCreate;
    private final int udsPrimaries;
    private final int udsSteps;
    private final boolean withUDS;
    private final int calculationTimeInNanoSeconds;

    private final ExecutorService endTestCheckerPool = Executors.newFixedThreadPool(1);

    private int counter;

    public UDSTester(int numberOfThreadsToCreate, int calculationTimeInNanoSeconds, int udsPrimaries, int udsSteps,
                     boolean withUDS) {
        this.numberOfThreadsToCreate = numberOfThreadsToCreate;
        this.calculationTimeInNanoSeconds = calculationTimeInNanoSeconds;
        this.udsPrimaries = udsPrimaries;
        this.udsSteps = udsSteps;
        this.withUDS = withUDS;
    }

    public Double runTest() {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        Callable<Double> testDoneRunnable = () -> {
            long start = System.currentTimeMillis();
            if(withUDS) {
                long numberOfThreadsSubmittedBeforeTest = UDScheduler.getInstance().getNumberOfThreadsScheduled();
                while(UDScheduler.getInstance().getNumberOfThreadsTerminated() < numberOfThreadsSubmittedBeforeTest + numberOfThreadsToCreate - udsPrimaries) {
                    try {
                        Thread.sleep(50);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                while(counter < numberOfThreadsToCreate) {
                    try {
                        Thread.sleep(50);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            double duration = (double) System.currentTimeMillis() - start;
            double throughput = (double) numberOfThreadsToCreate / (duration / 1000);
            logger.warning("UDSTester has submitted " + numberOfThreadsToCreate + " Threads.");
            logger.warning("Elapsed time: " + duration + "ms (± 50ms).");
            logger.warning("Approx. throughput: " + throughput + "threads/s");
            return throughput;
        };

        // start the statechecker thread
        Future<Double> throughputFuture = endTestCheckerPool.submit(testDoneRunnable);

        try {
            // logger.warning(counter + " threads did their work so far.");
            Thread.sleep(100);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }

        if(withUDS) {
            UDSLock udsLock  = new UDSLock(1);
            Runnable lu250LURunnableUDS = () -> {
                // lock and unlock the UDSLock
                udsLock.lock();
                udsLock.unlock();
                // simulate load
                simulateCPULoadNanos(calculationTimeInNanoSeconds);
                // lock and unlock the UDSLock again
                udsLock.lock();
                udsLock.unlock();
            };

            // reconfigure UDS
            UDScheduler.getInstance().addRequest(() -> UDScheduler.getInstance().requestReconfiguration(udsPrimaries,
                    udsSteps), () -> {});

            // create and run threads with UDS
            logger.warning("Tester {" + Thread.currentThread().getName() + "}: " +
                    "Adding " + numberOfThreadsToCreate + " threads to UDS ...");
            for(int j = 0; j < numberOfThreadsToCreate; j++) {
                UDScheduler.getInstance().addRequest(lu250LURunnableUDS, () -> {});
            }
        } else {
            ReentrantLock reentrantLock  = new ReentrantLock();
            Runnable lu250LURunnable = () -> {
                // lock and unlock the UDSLock
                reentrantLock.lock();
                reentrantLock.unlock();
                // simulate load
                simulateCPULoadNanos(calculationTimeInNanoSeconds);
                // lock and unlock the UDSLock again
                reentrantLock.lock();
                counter++;
                reentrantLock.unlock();
            };

            ExecutorService threadPool = Executors.newCachedThreadPool();
            // create and run threads
            logger.warning("Tester {" + Thread.currentThread().getName() + "}: " +
                    "Adding " + numberOfThreadsToCreate + " threads to CachedThreadPool ...");
            for(int j = 0; j < numberOfThreadsToCreate; j++) {
                threadPool.submit(lu250LURunnable);
            }
        }

        double throughput = -1d;
        try {
            throughput = throughputFuture.get();
        } catch(InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return throughput;
    }

    /**
     * Occupies the thread by spinning and counting how often it spun for a given number of nanoseconds (slightly
     * veriable depending on the current systems JVM's accuracy).
     *
     * @param durationInNanoseconds How long the thread should spin for, in ns
     */
    private void simulateCPULoadNanos(int durationInNanoseconds) {
        if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Simulating load for " + durationInNanoseconds + "ns.");
        }
        long startTime = System.nanoTime();
        while(System.nanoTime() - startTime < durationInNanoseconds) ;
        if(logger.isLoggable(Level.FINEST)) {
            logger.finest(Thread.currentThread().getName() + ": Finished simulating load (" + durationInNanoseconds +
                    "ns)");
        }
    }


    public static void main(String[] args) {

        if(args.length < 3) {
            System.out.println("Please supply numberOfThreadsToCreate, calculationTimeNs, Steps, and withUDS." +
                    " Example usage: $ UDSTester 100000 250000 2 true");
            System.exit(1);
        }

        int numberOfThreadsToCreate = Integer.parseInt(args[0]);
        int calculationTimeNs = Integer.parseInt(args[1]);
        int udsSteps = Integer.parseInt(args[2]);
        boolean withUDS = Boolean.parseBoolean(args[3]);

        Map<Integer, Double> results = new HashMap<>();
        if(withUDS) {
            // test for all of these UDS-primaries configurations
            int[] udsPrimaries = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20};
            for(int n : udsPrimaries) {
                UDSTester tester = new UDSTester(numberOfThreadsToCreate, calculationTimeNs, n, udsSteps, true);
                results.put(n, tester.runTest());

                try {
                    Thread.sleep(200);
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else {
            for(int i = 0; i < 3; i++) {
                UDSTester tester = new UDSTester(numberOfThreadsToCreate, calculationTimeNs, 1, 1, false);
                results.put(i, tester.runTest());
            }
        }

        System.out.println(results.keySet().stream()
                .sorted()
                .map((i) -> i + ": " + results.get(i))
                .collect(Collectors.joining(" | ", "Throughput per " + (withUDS ? " primary: [" : "run: ["), "]")));

        System.exit(0);
    }

}
