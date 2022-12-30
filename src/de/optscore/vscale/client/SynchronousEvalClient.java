package de.optscore.vscale.client;

import bftsmart.tom.ServiceProxy;
import com.lmax.disruptor.dsl.Disruptor;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.RequestProfile;
import de.optscore.vscale.RequestProfileRepository;
import de.optscore.vscale.util.EvalReqStatsClient;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class SynchronousEvalClient extends ServiceProxy implements EvalClient {
    private static final Logger logger = Logger.getLogger(SynchronousEvalClient.class.getName());

    private RequestProfile requestProfile;
    private long sendDelayNs;
    private RequestProfileRepository requestProfileRepository;

    private final Disruptor<EvalReqStatsClient> loggingDisruptor;
    private final ByteBuffer reqStatsBuffer;

    private final ReentrantLock activationLock;
    private final Condition shouldBeActive;
    private boolean clientActive;
    private boolean clientClosed;

    public SynchronousEvalClient(int procId, RequestProfile requestProfile, long sendDelayNs,
                                 RequestProfileRepository requestProfileRepository,
                                 ReentrantLock activationLock, Condition shouldBeActive,
                                 Disruptor<EvalReqStatsClient> ringBuffer) {
        super(procId);
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.requestProfile = requestProfile;
        this.sendDelayNs = sendDelayNs;
        this.requestProfileRepository = requestProfileRepository;

        // for publishing timing/log results to the logger via a Disruptor
        this.loggingDisruptor = ringBuffer;
        this.reqStatsBuffer = ByteBuffer.allocate(2 * Integer.BYTES + 2 * Long.BYTES);

        this.activationLock = activationLock;
        this.shouldBeActive = shouldBeActive;

        this.clientActive = false;
        this.clientClosed = false;

        // Workaround for stupid BFT-SMaRt bug:
        // Send one dummy request immediately after instantiation/connection, so the channel to this client is really
        // active on every replica.
        // Otherwise, the replicas might trip over themselves when too many clients are connecting at the same time
        // and channels aren't really active on the server side, which leads to "re-trying" errors and fudged benchmarks
        this.invokeOrdered(EvalRequest.serializeEvalRequest(
                new EvalRequest.EvalRequestBuilder().action(EvalActionType.READONLY.getActionTypeCode(), 0).build()));
    }

    @Override
    public void run() {
        logger.finest("ClientThread for SynchronousEvalClient #" + getProcessId() + " has been created.");

        // go to sleep and wait for notifications to wake up to send requests until finally the client gets closed

        while(!clientClosed) {

            // wait on the Condition until signalled by the managing ClientGroupInstance
            activationLock.lock();
            try {
                while(!clientClosed && !clientActive) {
                    logger.finest("Client " + getProcessId() + " sleeping, waiting for signal ...");
                    shouldBeActive.await();
                }
                logger.finest("Client " + getProcessId() + " woke up ...");
            } catch(InterruptedException e) {
                e.printStackTrace();
            } finally {
                activationLock.unlock();
            }

            // send requests unil the ClientGroupInstance tells the Client to stop again
            while(!clientClosed && clientActive) {
                sendRequest(requestProfileRepository.getRequestForProfile(requestProfile));
                if(sendDelayNs > 0) {
                    try {
                        Thread.sleep(sendDelayNs / 1000000);
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        // if we're here, client got woken up but clientClosed is true, so shutdownClient() has been called; end of test
        logger.info("Client " + getProcessId() + " thread ending; has sent "
                + operationId + " requests during its lifetime.");
    }

    private void sendRequest(EvalRequest request) {
        // tmp save time of before sending request, since opId of the request is not yet available
        long beforeSend = System.nanoTime() + ClientWorker.BENCHMARK_NANOTIME_OFFSET;

        // send the request to all replicas, and we don't actually care about the reply at the moment
        // this will set the operationId in the superclass, so we can access it after this call has returned
        this.invokeOrdered(EvalRequest.serializeEvalRequest(request));

        // measure time it took for the response to arrive
        long responseReceived = System.nanoTime() + ClientWorker.BENCHMARK_NANOTIME_OFFSET;

        // log the reqStats to Disk via the Disruptor
        reqStatsBuffer.putInt(this.getProcessId())
                .putInt(operationId)
                .putLong(beforeSend)
                .putLong(responseReceived)
                .flip();
        loggingDisruptor.publishEvent((event, sequence, bb) -> {
            event.setClientPid(bb.getInt());
            event.setOpId(bb.getInt());
            event.setSentTime(bb.getLong());
            event.setReceivedTime(bb.getLong());
            bb.clear();
        }, reqStatsBuffer);

    }

    @Override
    public void wakeUpClient() {
        logger.finest("Client " + getProcessId() + " is being woken up ...");
        this.clientActive = true;
    }

    @Override
    public void putClientToSleep() {
        logger.finest("Client " + getProcessId() + " going to sleep ...");
        this.clientActive = false;
    }

    @Override
    public void shutdownClient() {
        logger.finest("Client " + getProcessId() + " is being shut down and closed ...");
        this.clientClosed = true;
        this.close();
    }
}
