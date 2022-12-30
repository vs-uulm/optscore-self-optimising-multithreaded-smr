package de.uniulm.vs.art.uds;

import de.optscore.vscale.client.ClientWorker;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a UDS-aware Lock. If a thread requests/takes this lock,
 * a UDS-imposed total order will be obeyed.
 */
public class UDSLock implements Lock {

    /**
     * Reference to the UDS Scheduler instance of this JVM, so we can wait on its Conditions, etc
     */
    private final UDScheduler uds = UDScheduler.getInstance();

    /**
     * Identifier of this Lock
     */
    private final int lockID;

    /**
     * The thread currently holding this UDSLock, null if the lock is currently free.
     */
    private UDScheduler.UDSThread owner = null;

    /**
     * The per-mutex wait queue for threads which requested this Lock but have not yet acquired it.
     * May get cleared, e.g. at the end of a round.
     */
    private List<UDScheduler.UDSThread> enqueuedThreads = new LinkedList<>();

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(UDSLock.class.getName());


    /**
     * Creates a basic UDSLock
     */
    public UDSLock() {
        this(0);
    }

    public UDSLock(int id) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
        this.lockID = id;
    }

    /**
     * Obeys total order before granting a thread the lock
     */
    @Override
    public void lock() {
        uds.getSchedulerLock().lock();
        UDScheduler.UDSThread t = UDScheduler.getCurrentUDSThread();
        String prefix = t.getIdString() + " UDSLock.lock(): ";
        try {
            if(logger.isLoggable(Level.FINE)) {
                logger.fine(prefix + "locking L" + this.lockID);
            }

            // obey total order. Thread might get parked in UDScheduler conditions, but will eventually continue here..
            uds.waitForTurn();

            if(logger.isLoggable(Level.FINEST)) {
                logger.finest(prefix + "finds lock " + this.lockID + " with owner " +
                        (this.owner != null ? owner.getIdString() : "'nobody'"));
            }
            if(this.owner != null) {
                // If UDSLock is occupied, enqueue current thread and let it wait
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(prefix + "enqueues itself at lock L" + this.lockID);
                }
                this.enqueuedThreads.add(t);
                t.setEnqueued(this);

                // check if round is over, else continue by waiting until thread is first in mutex wait queue
                uds.checkForEndOfRound();
                while(this.owner != t && t.getEnqueued() != null) {
                    if(logger.isLoggable(Level.FINER)) {
                        logger.finer(prefix + "goes to sleep, waiting for being dequeued" +
                                " for lock L" + this.lockID);
                    }
                    t.awaitDequeueing();
                    if(logger.isLoggable(Level.FINER)) {
                        logger.finer(prefix + "was woken up by signal to isEnqueuedOrLockedOwnedByThread" +
                                " for lock L" + this.lockID);
                    }
                }
                if(this.owner != t) {
                    // repeat (see UDS v1.2.1 spec line 28)
                    // unlock first, then
                    if(logger.isLoggable(Level.WARNING)) {
                        logger.warning(prefix + "tried locking L" + this.lockID +
                                ", but wasn't right owner. Current owner is " + this.owner.getIdString() + ". Trying " +
                                "again");
                    }
                    lock();
                }
            } else {
                // take this UDSLock
                if(logger.isLoggable(Level.FINER)) {
                    logger.finer(prefix + "took lock L" + this.lockID);
                }
                this.owner = t;
            }
            uds.setProgress(true);
        } catch(Throwable e) {
            e.printStackTrace();
            logger.info(prefix + "has been interrupted while waiting on " +
                    "isEnqueuedOrLockOwnedByThread ...");
        } finally {
            uds.getSchedulerLock().unlock();
        }
    }

    /**
     * Unlocks this UDSLock and wakes up enqueued threads if applicable,
     * so they can try to acquire the lock while obeying the total order.
     */
    @Override
    public void unlock() {
        uds.getSchedulerLock().lock();
        try {
            UDScheduler.UDSThread t = UDScheduler.getCurrentUDSThread();
            if(logger.isLoggable(Level.FINE)) {
                logger.fine(t.getIdString() + " UDSLock.unlock(): is going to release " + this.lockID);
            }

            if(t != this.owner) {
                // a thread not currently owning the mutex tried to unlock it. Return and ignore the request.
                throw new IllegalMonitorStateException(t.getIdString() + " UDSLock.unlock(): tried to " +
                		"release " + this.lockID + " even though it wasn't the owner");
            }

            this.owner = null;

            // see if  other threads are waiting for this UDSLock and grant it to the first thread in queue
            if(enqueuedThreads.size() > 0) {
                if(logger.isLoggable(Level.FINEST)) {
                    logger.finest(t.getIdString() + " UDSLock.unlock(): checks enqueuedThreads" +
                            " for lock " + this.lockID + " and waking them up");
                }
                UDScheduler.UDSThread next = enqueuedThreads.get(0);
                this.owner = next;

                // signal the one thread that next will run due to changed condition
                if(logger.isLoggable(Level.FINE)) {
                    logger.fine(t.getIdString() + " UDSLock.unlock(): signaling dequeued thread");
                }
                next.dequeueThread();
            }

            if(logger.isLoggable(Level.FINEST)) {
                logger.finest(t.getIdString() + " UDSLock.unlock(): released lock " + this.lockID + "; new owner is " +
                        (this.owner != null ? owner.getIdString() : "'nobody'"));
            }
        } finally {
            uds.getSchedulerLock().unlock();
        }
    }

    /**
     * Not implemented for UDSLock. Do not use.
     * Use lock() instead.
     * @return
     * @throws InterruptedException
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new NoSuchMethodError();
    }

    /**
     * Not implemented for UDSLock. Do not use.
     * Use lock() instead.
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock() {
        throw new NoSuchMethodError();
    }

    /**
     * Not implemented for UDSLock. Do not use.
     * Use lock() instead.
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        throw new NoSuchMethodError();
    }


    /**
     * Not implemented for UDSLock. Do not use.
     */
    @Override
    public Condition newCondition() {
        throw new NoSuchMethodError();
    }

    /**
	 * Remove a thread from the queue of this lock
	 * @param t The thread to be removed
     */
    public void removeFromQueue(UDScheduler.UDSThread t) {
        if(logger.isLoggable(Level.FINE)) {
            logger.fine( UDScheduler.getCurrentUDSThread().getIdString() + " removeFromQueue(): is removing " +
                    t.getIdString() + " from queue of lock " + this.lockID );
        }
        while(enqueuedThreads.remove(t)) { }
    }
}
