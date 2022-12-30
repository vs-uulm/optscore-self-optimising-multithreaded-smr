package de.uniulm.vs.art.uds;

import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.util.EvalReqStatsServer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * UDS Scheduler implementation as a Singleton.
 */
public class UDScheduler {

    /**
     * Ordered list of all threads in the application that were or are to be scheduled by UDS
     */
    private final List<UDSThread> threads;

    /**
     * Primary threads in the current round
     */
    private List<UDSThread> primaries;

    /**
     * The single devliery thread that waits for admission;
     */
    private UDSThread admissionThread;
    
    /**
     * Current round, incremented when a new round is started
     */
    private int round;

    /**
     * The number of threads that have already been 'seen' by UDS
     * TODO: deal with wraparounds after MAX_INT threads have been created/seen
     */
    private int highestThreadNo;

    /**
     * The next UDS thread ID
     * TODO: deal with wraparounds after MAX_INT threads have been created
     */
    private AtomicInteger threadID = new AtomicInteger(0);

    /**
     * Indicator whether any progress was made in the current round
     */
    private boolean progress = false;

    /**
     * The total number of threads that have been added to UDS since it started scheduling
     */
    private long numberOfThreadsScheduled;

    /**
     * The total number of threads that have been scheduled with UDS that have terminated themselves (= completed)
     */
    private long numberOfThreadsTerminated;

    /**
     * Thread-local variable referring to current UDS thread
     */
    private static final ThreadLocal<UDSThread> currentUDSThread = new ThreadLocal<>();

    /**
     * Holds all values required to configure UDS; used by UDS methods to read config during rounds, so should never
     * be changed by outside methods, only by UDS' own reconfigure()-method.
     */
    private final UDSConfiguration udsConfiguration = new UDSConfiguration();

    /**
     * A freely modifiable copy of UDS config, which is used for changing the actual UDS config during
     * reconfigurigation (inbetween rounds) by copying its contents.
     */
    private final UDSConfiguration requestedUDSConfiguration = new UDSConfiguration();

    /**
     * Thread pool for request scheduling
     */
    private final ExecutorService udsThreadPool = Executors.newCachedThreadPool();

    /**
     * Use ReentrantLock so we have access
     * to its Condition Objects and can await/signal threads efficiently.
     */
    private final ReentrantLock schedulerLock = new ReentrantLock();

    /*
     * Global wait conditions for UDS
     * Thread-local conditions are managed within UDSThread
     */
    private final Condition isFinished = schedulerLock.newCondition();
    private final Condition threadExists = schedulerLock.newCondition();

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(UDScheduler.class.getName());

    /**
     * Singleton. Not instantiable.
     */
    private UDScheduler() {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.threads = new LinkedList<>();
        this.primaries = new ArrayList<>(64);
        this.admissionThread = null;
        this.round = 0;
        this.numberOfThreadsScheduled = 0;
        this.numberOfThreadsTerminated = 0;

        // set initial configuration 1 prim 1 step
        // set new number of primaries
        this.requestedUDSConfiguration.setN(1);

        int[] test = new int[]{1, 2};

        // create and set new total order
        List<Integer> newTotalOrder = new ArrayList<Integer>(1);
        newTotalOrder.add(0);
        this.requestedUDSConfiguration.setTotalOrder(newTotalOrder);
    }


    /**
     * Singleton pattern
     */
    private static class LazySingletonHolder {
        static final UDScheduler instance = new UDScheduler();
    }

    /**
     * Get the Singleton instance of this UDS Scheduler
     *
     * @return Singleton UDS Scheduler instance
     */
    public static UDScheduler getInstance() {
        return LazySingletonHolder.instance;
    }

    /**
     * Get current UDS Thread
     */
    public static UDSThread getCurrentUDSThread() {
        return currentUDSThread.get();
    }

    /**
     * Adds a request (= Runnable) to the UDS scheduling queue. Blocks(!) until the internal UDS thread list is
     * empty enough so the new thread can be added and started.
     * Threads are started by this method, and then scheduled with UDS.
     *
     * @param r Runnable responsible for fulfilling a client request, added to UDS for scheduling purposes.
     */
    public void addRequest(Runnable r, Runnable replyRunnable) {
        // Take schedulerLock to guard against another thread messing with the
        // thread queue while we add and start the new one
        schedulerLock.lock();
        String prefix= "{" + Thread.currentThread().getName() + "} addRequest: ";
        try {
            /*
             * Create the UDS thread and its runnable with boiler plate code for correct termination
             */
            UDSThread thread = new UDSThread();
            thread.initTask(r, replyRunnable);

            if(logger.isLoggable(Level.FINE)) {
                logger.fine(prefix + "processing thread " + thread.getIdString());
            }

            // Implementation of back-pressure if there are too many pending threads in the system
            // TODO: Maximum size is a good guess but far from being evaluated or tuned
            if(this.threads.size() > ((udsConfiguration.getN() * 2) + 50)) {
                if(logger.isLoggable(Level.FINEST)) {
                    logger.finest(prefix + "blocking due to too many threads");
                }
                admissionThread = thread;
                thread.awaitAdmission();                	
            }

            // First the thread is added to the thread list to preserve order
            if(logger.isLoggable(Level.FINEST)) {
                logger.finest(prefix + "adding thread " + thread.getIdString() + " to UDS threads");
            }
            this.threads.add(thread);
            numberOfThreadsScheduled++;

            if(logger.isLoggable(Level.FINER)) {
                String threadListStatus = threads.stream().map(UDSThread::getIdString).
                        collect(Collectors.joining(", ", prefix + "current threadList: [", "]"));
                logger.finer(threadListStatus);
            }

            // start the thread
            if(logger.isLoggable(Level.FINEST)) {
                logger.finest(prefix + "starting " + thread.getIdString());
            }
            thread.start();

            // signal scheduling thread waiting for threads in startRound().
            // Should be only one thread awaiting this condition, but call signalAll() just to be safe.
            threadExists.signalAll();
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Method for adding a UDSRequest and immediately filling the current round, so that it can end and start the
     * next one. Useful for e.g. reconfiguration decisions for autoscaling, etc.
     */
    public void addRequestAndFillRound(Runnable r, Runnable replyRunnable) {
        // first add the request as always
        addRequest(r, replyRunnable);

        // if the current round is not yet full and we don't have enough threads in the thread list that could fill it,
        // add dummy requests until the round can be started
        int mt = udsConfiguration.getN() - (threads.size() + primaries.size());
        for(int i = 0; i < mt; i++) {
            addRequest(() -> {}, () -> {});
        }
    }

    /******************************************************
     ***************** Scheduling Methods *****************
     ******************************************************/

    /**
     * Start a new scheduling round. Can only be called by scheduler methods, not publically.
     */
    private void startRound() {
        int i;
        int n;
        String prefix = "";
        if(logger.isLoggable(Level.FINE)) {
            prefix = currentUDSThread.get().getIdString() + " startRound(): ";
        }

        schedulerLock.lock();
        try {
            // increment round counter
            this.round++;

            // empty the set of primaries
            primaries.clear();
            
            // reconfigure UDS
            reconfigure(progress);
            if(logger.isLoggable(Level.FINE)) {
            	logger.info(prefix + "starting round " + this.round + ", total order " +
            			totalOrderToString() + ", with " + udsConfiguration.getN() + " primaries by adding threads to " + 
            			"primaries and signaling them");
            }

            // fill with threads
            for(i = 0, n = 0; true; i++) {
                while(threads.size() <= i || !threads.get(i).isStarted()) {
                    if(logger.isLoggable(Level.FINER)) {
                        logger.finer(prefix + " waiting for new threads (round " + this.round + ")");
                    }
                    try {
	                    threadExists.await();
                    } catch( InterruptedException e ) {
                        logger.info(prefix + "has been interrupted while waiting on " +
                                "threadExists. Re-waiting ...");
                    }
                }
                if(logger.isLoggable(Level.FINER)) {
                    logger.finer(prefix + "was woken up by signal to threadExists (Round " + this.round + ")");
                }
                UDSThread t = threads.get(i);

                // If a thread is not already terminated and the prim()-predicate allows it, add thread to primaries
                if(!t.isTerminated() && prim(t)) {
                    if(logger.isLoggable(Level.FINER)) {
                        logger.finer(prefix + "adding thread " + t.getIdString() + " to primaries");
                    }
                    t.setPrimary(true);
                    this.primaries.add(t);

                    if(logger.isLoggable(Level.FINER)) {
                        String primariesStatus = primaries.stream().map(UDSThread::getIdString).
                                collect(Collectors.joining(", ", prefix + "new primaries [", "]"));
                        logger.finer(primariesStatus);
                    }

                    // If we have enough primaries for this round, stop looking for more primaries
                    if(++n >= udsConfiguration.getN()) {
                        break;
                    }
                }
            }


            // update the number of seen threads
            if(highestThreadNo < i) {
                if(logger.isLoggable(Level.FINEST)) {
                    logger.finest(prefix + "increased highestThreadNo to " + i);
                }
                highestThreadNo = i;
            }
            
            // prune thread list by removing all terminated threads
            int removedThreads = threads.size();
            if(threads.removeIf(UDSThread::isTerminated)) {
            	// some space for new threads

            	// compute removed threads and correct number of seen threads
            	removedThreads -= threads.size();
            	highestThreadNo -= removedThreads;
                if(logger.isLoggable(Level.FINEST)) {
                    logger.finest(prefix + "corrected highestThreadNo to " + highestThreadNo);
                }

            	// admit waiting delivery thread
            	if( admissionThread != null ) {
            		admissionThread.signalAdmission();
                    if(logger.isLoggable(Level.FINER)) {
                        logger.finer(prefix + "admitted thread " + admissionThread.getIdString());
                    }
            		admissionThread= null;
            	}
            }
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Checks whether the current scheduling round meets all conditions to end.
     */
    public void checkForEndOfRound() {
        schedulerLock.lock();
        String prefix = getCurrentUDSThread().getIdString() + " checkForEndOfRound(): ";
        
        try {
            // check all primaries whether they are finished/terminated/waiting
            if(logger.isLoggable(Level.FINE)) {
                logger.fine(prefix +"round " + round);
            }

            // if there are currently less primaries than there should be (as per the udsConfiguration
            // for the current round), then the round is not yet over and we have to wait for more primaries
            if(primaries.size() < n(round)) {
                if(logger.isLoggable(Level.FINER)) {
                    logger.finer(prefix + "did not yet see all primaries");
                }
                return;
            }

            if(logger.isLoggable(Level.FINEST)) {
                logger.finest(prefix + "checks status of all primaries");
            }
            for(UDSThread t : primaries) {
                // if any primary is still running/has steps/is waiting for its turn, round is not over
                if(logger.isLoggable(Level.FINEST)) {
                    logger.finest(t.toString());
                }
                if( !t.isTerminated()
                		&& t.getEnqueued() == null
                        && !t.isFinished()
                        && !t.isWaitingForTurn()) {
                    if(logger.isLoggable(Level.FINE)) {
                        logger.fine(prefix + "round " + round + " not yet over");
                    }
                    return;
                }
            }
            // end of round detected
            if(logger.isLoggable(Level.FINE)) {
                logger.fine(prefix + "end of round " + round + " detected");
            }
            for(UDSThread t : primaries) {
                t.setPrimary(false);
                t.setFinished(false);
                t.setWaitingForTurn(false);
                t.dequeueThread();
            }
            // reset primaries
            primaries.clear();

            // signal waiting threads to re-check their changed conditions
            // don't signal primaries, since we cleared primaries and no thread could possibly be primary
            if(logger.isLoggable(Level.FINER)) {
                logger.finer(prefix + "signaling threads waiting on isFinished to start new round");
            }
            isFinished.signalAll();

            // ... and now start a new round
            startRound();
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Called whenever total order needs to be obeyed by a UDSThread.
     */
    public void waitForTurn() {
        schedulerLock.lock();
        UDSThread t = getCurrentUDSThread();
        String logPrefix = t.getIdString() + " waitForTurn(): ";
        try {
            // bootstrap
            if(round == 0) {
                setProgress(true);
                startRound();
            }

            while(true) {
                // Wait until primary
                if(logger.isLoggable(Level.FINER)) {
                    logger.finer(logPrefix + "before waitForPrimary()");
                }
                t.waitForPrimary();

                if(logger.isLoggable(Level.FINEST)) {
                    logger.finest(logPrefix + "checking whether there are any steps left in the total order ("
                            + totalOrderToString() + ")");
                }

                // check whether we have any steps left in total order
                if(!udsConfiguration.getTotalOrder().contains(primaries.indexOf(t))) {
                    t.setFinished(true);
                    checkForEndOfRound();
                    // The round is not yet over, but we have no steps in the total order. So wait for
                    // the signal isFinished = false, which means the next round has started and we can try again.
                    while(t.isFinished()) {
                        if(logger.isLoggable(Level.FINER)) {
                            logger.finer(logPrefix + "before awaiting isFinished");
                        }
                        isFinished.await();
                        if(logger.isLoggable(Level.FINER)) {
                            logger.finer(logPrefix + "after woken up by signal to isFinished");
                        }
                    }
                } else if(udsConfiguration.getTotalOrder().size() > 0
                        && primaries.size() > udsConfiguration.getTotalOrder().get(0)
                        && primaries.get(udsConfiguration.getTotalOrder().get(0)).equals(t)) {
                    // if current thread is first in total order, it may continue and remove the step from total order
                    if(logger.isLoggable(Level.FINE)) {
                        logger.fine(logPrefix + "is removing its step from the tip of the total order");
                    }

                    udsConfiguration.getTotalOrder().remove(0);

                    if(logger.isLoggable(Level.FINER)) {
                    	logger.finer(logPrefix + "total order left is " + totalOrderToString());
                    }

                    // wake up next thread in total order if still some steps left and if enough primaries
                    if(udsConfiguration.getTotalOrder().size() > 0
                            && primaries.size() > udsConfiguration.getTotalOrder().get(0)) {
                        primaries.get(udsConfiguration.getTotalOrder().get(0)).setWaitingForTurn(false);
                    }
                    break;
                } else {
                    // wait for step / first in total order
                    t.setWaitingForTurn(true);
                    checkForEndOfRound();
                    while(t.isWaitingForTurn()) {
                        if(logger.isLoggable(Level.FINER)) {
                            logger.finer(logPrefix + " waitForTurn(): going to sleep awaiting turn");
                        }
                        t.awaitTurn();
                        if(logger.isLoggable(Level.FINER)) {
                            logger.finer(logPrefix + " waitForTurn(): woken up by signal to isWaitingForTurn");
                        }
                    }
                }
            }
        } catch(InterruptedException e) {
            logger.info(logPrefix + " waitForTurn(): has been interrupted while waiting on " +
                    "isPrimary. Re-waiting ...");
        } catch(ClassCastException e) {
            logger.severe(logPrefix + " waitForTurn(): tried to cast a non-UDSThread to UDSThread");
            System.exit(1);
        } finally {
            schedulerLock.unlock();
        }
    }

    private void removeFromOrder(UDSThread t) {
        // remove all steps of t in total order
        Integer i = primaries.indexOf(t);
        if(logger.isLoggable(Level.FINER)) {
            logger.finer(t.getIdString() + " removeFromOrder(): removes all its steps from the total order");
        }
        while(udsConfiguration.getTotalOrder().remove(i)) {}
        if(logger.isLoggable(Level.FINEST)) {
        	logger.finest(t.getIdString() + " removeFromOrder: new total order " +
        					totalOrderToString() );
        }
        // signal next thread in total order if there are steps left in this round
        if(udsConfiguration.getTotalOrder().size() > 0
                && primaries.size() > udsConfiguration.getTotalOrder().get(0)) {
            schedulerLock.lock();
            try {
                primaries.get(udsConfiguration.getTotalOrder().get(0)).setWaitingForTurn(false);
            } finally {
                schedulerLock.unlock();
            }
        }
    }

    /**
     * Called inbetween rounds in order to reconfigure UDS.
     * Copies contents of requestedUDSConfiguration, which means a true reconfiguration can either be triggered from
     * the outside or by the scheduler itself, by changing requestedUDSConfiguration before this method is called
     */
    private void reconfigure(boolean progress) {
        schedulerLock.lock();
        String prefix = UDScheduler.currentUDSThread.get().getIdString() + " reconfigure(): ";
        if(logger.isLoggable(Level.FINE)) {
            logger.fine(prefix + "start reconfiguration");
        }
        try {
            // reconfigure UDS by copying contents of requestedUDSConfiguration
            udsConfiguration.setN(requestedUDSConfiguration.getN());
            udsConfiguration.setTotalOrder(new ArrayList<>(this.requestedUDSConfiguration.getTotalOrder()));
            if(logger.isLoggable(Level.FINER)) {
            	String totalOrderStatus = udsConfiguration.getTotalOrder().stream().map(Object::toString).
            			collect(Collectors.joining(", ", prefix +
            					"new total order [", "] and " + udsConfiguration.getN() +
            					" primaries"));
            	logger.finer(totalOrderStatus);
            }

            // TODO if no progress was made, increase number of primaries for next round
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Called by UDSThreads when they've finished their Runnable task
     * and want to terminate. Can only be called by the terminating thread itself.
     *
     * @param t The thread who wants to terminate, can be null if not known
     */
    protected void terminateThread(UDSThread t) {
        schedulerLock.lock();
        if( t == null )
            t = currentUDSThread.get();

        if(logger.isLoggable(Level.FINE)) {
            logger.fine(t.getIdString() + " terminateThread()" );
        }
        try {
        	// If we haven't had any critical ops yet, start a round so we don't wait forever on becoming primary
        	if(round == 0) {
        		startRound();
        	}
        	setProgress(true);

            if(logger.isLoggable(Level.FINER)) {
                logger.finer(t.getIdString() + " terminateThread(): before waitForPrimary()");
            }
            t.waitForPrimary();
            
            // Thread can be marked as terminated and retire itself
            t.setTerminated(true);
            removeFromOrder(t);
            checkForEndOfRound();
            numberOfThreadsTerminated++;
            // nothing left to do, thread should stop itself after returning
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Predicate function for deciding whether a thread can be added to primaries.
     * Terminated threads are obviously out.
     * Can be used for fine-tuning UDS.
     *
     * @param udsThread the thread that is trying to become primary
     * @return true if the thread may become primary, false otherwise
     */
    private boolean prim(UDSThread udsThread) {
        return true;
    }

    /**
     * Returns the number of primaries for the current round
     * // TODO properly implement
     *
     * @param round unused atm
     * @return number of primaries for a round
     */
    private int n(int round) {
        return udsConfiguration.getN();
    }

    /**
     * Determines where newly created threads are inserted in the thread list.
     *
     * @return index in the thread list for a new thread (usually the highestThreadNo)
     */
    private int threadPosition() {
        return this.highestThreadNo;
    }

    /**
     * Specifies whether newly created threads are to be put into primaries directly upon creation
     *
     * @return true if new threads are immediately added to primaries, false if otherwise
     * TODO maybe refactor and move this to UDSConfiguration
     */
    private boolean newAsPrimary() {
        return false;
    }

    public void setProgress(boolean progress) {
        this.progress = progress;

    }

    public ReentrantLock getSchedulerLock() {
        return schedulerLock;
    }

    /**
     * Tell UDS to reconfigure itself to use the specified number of primaries, each getting stepsPerPrimary steps in
     * new rounds, with a "round robin" total order
     *
     * @param primaries       The number of primaries that should be used in all following rounds that are started
     * @param stepsPerPrimary The number of steps each primary receives in all following rounds
     */
    int requestReconfiguration(int primaries, int stepsPerPrimary) {
        schedulerLock.lock();
        // TODO implement different total orders (all at once, random, etc)
        // TODO sanity checks on input parameters (e.g. no prims/steps < 0, etc)

        // wait for turn so we stay deterministic when reconfiguring UDS
        waitForTurn();

        if(logger.isLoggable(Level.WARNING)) {
            logger.warning("{" + Thread.currentThread().getName() + "} requestConfiguration: " +
                    "new UDS configuration requested (" + primaries + " " +
                    "prims, " + stepsPerPrimary + " steps per prim)");
        }
        try {
            // set new number of primaries
            this.requestedUDSConfiguration.setN(primaries);

            // create and set new total order
            List<Integer> newTotalOrder = new ArrayList<>(primaries * stepsPerPrimary + 1);
            for(int i = 0; i < stepsPerPrimary; i++) {
                for(int j = 0; j < primaries; j++) {
                    newTotalOrder.add(j);
                }
            }
            this.requestedUDSConfiguration.setTotalOrder(newTotalOrder);
        } finally {
            schedulerLock.unlock();
        }

        // return new number of primaries
        return this.requestedUDSConfiguration.getN();
    }

    /**
     * Reconfigure only the primaries, while keeping the steps from the previous configuration.
     * See
     * @param primaries
     * @return
     */
    public int requestReconfigurationPrimaries(int primaries) {
        // get the current number of steps (implicitly encoded in the length of the total order)
        int currentSteps = requestedUDSConfiguration.getTotalOrder().size() / requestedUDSConfiguration.getN();
        // request the new configuration
        return requestReconfiguration(primaries, currentSteps);
    }

    public void requestReconfigurationSteps(int steps) {
        // get the current number of primaries
        int currentPrimaries = requestedUDSConfiguration.getN();
        // request the new configuration
        requestReconfiguration(currentPrimaries, steps);
    }

    /**
     * Create log string with total order
     */
    private String totalOrderToString() {
    	return udsConfiguration.getTotalOrder().stream().map(Object::toString).
    				collect(Collectors.joining(", ", "[", "] is ")) +
    		   udsConfiguration.getTotalOrder().stream().map(
    					(Object o)-> { 	try {
    										return primaries.get((Integer)o).getIdString();
    									}
    									catch (IndexOutOfBoundsException e) {
    										return "(not yet known)";
    									}
    								 }
    				).
    				collect(Collectors.joining(", ", "[", "]"));    				
    }

    /**
     * Can be used for debugging / profiling purposes, e.g. to check how many rounds UDS has started in an external
     * thread which waits for a certain number of rounds
     * @return Current UDS round number
     */
    public int getCurrentRoundNumber() {
        return round;
    }

    public int getCurrentUDSConfigurationPrimaries() {
        return udsConfiguration.n;
    }

    public long getNumberOfThreadsScheduled() {
        return numberOfThreadsScheduled;
    }

    public long getNumberOfThreadsTerminated() {
        return numberOfThreadsTerminated;
    }

    /**
     * The current configuration of the UDS Scheduler.
     * Should only be changed via reconfigure().
     */
    private static class UDSConfiguration {

        /**
         * Number of primaries in the current round
         */
        private int n;

        /**
         * The total order of the current round
         */
        private List<Integer> totalOrder;

        /**
         * Create a basic configuration with 1 primary and 1 step
         */
        private UDSConfiguration() {
            this.n = 1;
            this.totalOrder = new ArrayList<>(2);
            totalOrder.add(0);
        }

        private int getN() {
            return n;
        }

        private void setN(int n) {
            this.n = n;
        }

        private List<Integer> getTotalOrder() {
            return totalOrder;
        }

        private void setTotalOrder(List<Integer> totalOrder) {
            this.totalOrder = totalOrder;
        }
    }


    /**
     * UDS's own thread class to manage UDS threads
     * A special inner class representing  of Thread which obeys UDS specifics.
     * For example, it executes terminate() before stopping, which
     * waits until the Thread is in the set of primaries before returning.
     */
    protected class UDSThread {
        /**
         * Thread ID
         */
        private final int id;

        /**
         * e.g. for logging purposes
         */
        private final String idString;

        /**
         * The task of this thread
         */
        private Runnable task;

        /**
         * True if thread was already started by UDS
         */
        private boolean started;

        /**
         * The lock the thread is currently enqueued for waiting for lock acquisition
         */
        private UDSLock enqueued;

        /**
         * True if thread is primary
         */
        private boolean primary;

        /**
         * Condition for scheduler's lock to wait until thread becomes primary
         */
        private Condition isPrimaryCondition;

        /**
         * Condition for back-pressure/thread admission
         */
        private final Condition admissionCondition;

        /**
         * Condition for being enqueued at a lock
         */
        private final Condition enqueuedCondition;
        
        /**
         * True if thread has terminated its usual processing, but is not yet removed by UDS
         */
        private boolean terminated;

        /**
         * True if thread has finished its round
         */
        private boolean finished;

        /**
         * True if thread waits for its turn in the total UDS order
         */
        private boolean waitingForTurn;

        /**
         * Condition for scheduler's lock to wait until it's the thread's turn
         */
        private Condition waitingForTurnCondition;

        /**
         * Constructor of UDS threads
         */
        UDSThread() {
            this.id = threadID.getAndIncrement();
            this.idString = "(T" + id + ")";
            this.started = false;
            this.enqueued = null;
            this.terminated = false;
            this.finished = false;
            this.waitingForTurn = false;

            // Create and intialise condition objects
            isPrimaryCondition = schedulerLock.newCondition();
            waitingForTurnCondition = schedulerLock.newCondition();
            admissionCondition = schedulerLock.newCondition();
            enqueuedCondition = schedulerLock.newCondition();

            if(logger.isLoggable(Level.FINEST)) {
                logger.finest( getIdString() + " UDSThread(): was created" );
            }
        }

        public String getIdString() {
            return idString;
        }

        boolean isStarted() {
            return started;
        }

        public UDSLock getEnqueued() {
            return this.enqueued;
        }

        public void setEnqueued(UDSLock l) {
            this.enqueued = l;
        }

        boolean isPrimary() {
            return this.primary;
        }

        void setPrimary(boolean primary) {
        	boolean oldPrimary= this.primary;
            this.primary = primary;
            if(logger.isLoggable(Level.FINEST)) {
                logger.finest( getIdString() + " setPrimary(): from "+oldPrimary+" to "+this.primary);
            }

        	if(!oldPrimary && primary)
                isPrimaryCondition.signal();
        }

        /**
         * Wait to become a primary thread.
         * This method can only be called when holding the scheduler lock.
         * It further has to be called on the currently running UDSThread
         */
        void waitForPrimary() {
            if(logger.isLoggable(Level.FINER)) {
                logger.finer(getIdString() + " waitForPrimary()");
            }
            while(!primary) {
                try {
                    isPrimaryCondition.await();
                } catch(InterruptedException e) {
                    logger.info(getIdString() +
                            " waitForPrimary: has been interrupted while waiting to become primary. Re-waiting...");
                }
            }
            if(logger.isLoggable(Level.FINER)) {
                logger.finer(getIdString() + " waitForPrimary(): became primary");
            }
        }

        void signalAdmission() {
            admissionCondition.signal();
        }

        void awaitAdmission() {
            if(logger.isLoggable(Level.FINER)) {
                logger.finer("{" + Thread.currentThread().getName() + "} awaitAdmission(): waits for admission of thread " + getIdString());
            }
           	while(true) {
           		try {
           			admissionCondition.await();
           			break;
           		} catch (InterruptedException e) {
           			logger.finest("{" + Thread.currentThread().getName() + 
						"} awaitAdmission(): was interrupted, re-waiting for admission");
           		}
           	}
            if(logger.isLoggable(Level.FINER)) {
                logger.finer("{" + Thread.currentThread().getName() + "} awaitAdmission: thread " + getIdString() +
                        " admitted");
            }
        }

        /**
         * Wait for being dequeued at a lock. Can only be called when holding the scheduler lock.
         */
        void awaitDequeueing() {
            if(logger.isLoggable(Level.FINER)) {
                logger.finer(getIdString() + " awaitDequeueing(): waits for dequeueing");
            }
           	while(true) {
        		try {
        			enqueuedCondition.await();
        			break;
        		} catch (InterruptedException e) {
        			logger.finest( getIdString() + "awaitDequeueing(): was interrupted, re-waiting for dequeueing");
        		}
        	}
        }

        /**
         * Dequeue this thread from its lock. Can only be called when holding the scheduler lock.
         */
        void dequeueThread() {
        	if(enqueued != null) {
                if(logger.isLoggable(Level.FINER)) {
                    logger.finer(getIdString() + " dequeueThread(): is dequeueing itself");
                }
            	enqueued.removeFromQueue(this);
                enqueued = null;
               	enqueuedCondition.signal();
            }      	
        }

        boolean isTerminated() {
            return this.terminated;
        }

        void setTerminated(boolean terminated) {
            this.terminated = terminated;
        }

        public boolean isFinished() {
            return this.finished;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
        }

        boolean isWaitingForTurn() {
            return this.waitingForTurn;
        }

        /**
         * This method can only be called when holding the scheduler lock
         */
        public void setWaitingForTurn(boolean waitingForTurn) {
            if(this.waitingForTurn && !waitingForTurn)
                waitingForTurnCondition.signal();
            this.waitingForTurn = waitingForTurn;
        }

        public void setWaitingForTurnCondition(Condition con) {
            this.waitingForTurnCondition = con;
        }

        /**
         * This method can only be called when holding the scheduler lock
         */
        void awaitTurn() {
            while(waitingForTurn) {
                try {
                    waitingForTurnCondition.await();
                } catch(InterruptedException e) {
                    logger.info(Thread.currentThread().getName() + "{" + id +
                            "} has been interrupted while waiting for turn. Re-waiting...");
                }
            }
        }

        @Override
        public String toString() {
            return "Thread " + getIdString() + ": " +
                    (isPrimary() ? "primary" : "not primary") + ", " +
                    (isTerminated() ? "terminated " : (
                    		(isFinished() ? "finished round" : "") + 
                    		(enqueued!=null ? "is enqueued" : "") +
                    		(isWaitingForTurn() ? "waits for turn" : "running") ) );
        }

        /**
         * Initialise UDS thread's task
         *
         * @param r the task to run
         */
        void initTask(Runnable r, Runnable replyRunnable) {
            task = () -> {
                try {
                    currentUDSThread.set(this);
                    r.run();
                    udsThreadPool.submit(replyRunnable);
                } finally {
                    UDScheduler.getInstance().terminateThread(this);
                }
            };
        }

        /**
         * Start the UDS thread
         */
        public void start() {
            udsThreadPool.submit(task);
            started = true;
        }
    }
}
