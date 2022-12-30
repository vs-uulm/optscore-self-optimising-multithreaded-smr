package de.optscore.vscale.server;

import bftsmart.parallelism.MessageContextPair;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.TOMUtil;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.util.EvalReqStatsServer;
import de.uniulm.vs.art.uds.UDScheduler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Server class of our system evaluation efforts. Starts a replica
 * and listens for EvalRequests. Basically just dispatches incoming requests to the UDScheduler.
 */
public class UDSServiceReplica extends ServiceReplica {


    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(UDSServiceReplica.class.getName());

    private EvalServer evalServer;

    private AtomicInteger globalReqSequence = new AtomicInteger(0);

    public UDSServiceReplica(int id, Executable executor, Recoverable recoverer) {
        super(id, executor, recoverer);

        // only possible because UDSServiceReplica is for benchmarking and should at the moment only be instantiated
        // by EvalServer
        try {
            this.evalServer = (EvalServer) executor;
        } catch(ClassCastException e) {
            e.printStackTrace();
            logger.severe("Could not cast executor to EvalServer (for stats logging). UDSServiceReplica is only for " +
                    "benchmarking and should not be used/instantiated by classes other than EvalServer");
            System.exit(3);
        }

        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
    }

    @Override
    public void receiveMessages(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs, TOMMessage[][] requests) {
        int consensusCount = 0;
        int requestCount;
        boolean noop;

        // loop through array of delivered message batches
        for (TOMMessage[] requestsFromConsensus : requests) {
            logger.finest("Received decision with " + requestsFromConsensus.length +" request(s) from DeliveryThread");
            TOMMessage firstRequest = requestsFromConsensus[0];
            requestCount = 0;
            noop = true;

            // TODO temp code to test dummyReqInsertion
            int udsReqCount = 0;

            // loop through a batch of messages
            for (TOMMessage request : requestsFromConsensus) {

                // bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Processing TOMMessage from
                // client " + request.getSender() + " with sequence number " + request.getSequence() + " for session " + request.getSession() + " decided in consensus " + consId[consensusCount]);


                // if the request has the correct view ID and is to be handled as an ordered request, proceed
                // normally, which is in this case means: Schedule the request as a thread with UDS
                if (request.getViewID() == SVController.getCurrentViewId() &&
                        request.getReqType() == TOMMessageType.ORDERED_REQUEST) {

                    noop = false;

                    MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(),
                    request.getReqType(), request.getSession(), request.getSequence(), request.getOperationId(),
                    request.getReplyServer(), request.serializedMessageSignature, firstRequest.timestamp,
                    request.numOfNonces, request.seed, regencies[consensusCount], leaders[consensusCount],
                    consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, false);


                    if (requestCount + 1 == requestsFromConsensus.length) {
                        msgCtx.setLastInBatch();
                    }

                    // TODO do we really want to modify/use msgCtx for this?
                    msgCtx.setGlobalReqSequence(globalReqSequence.getAndIncrement());
                    msgCtx.setEvalReqStatsServer(new EvalReqStatsServer());
                    // profiling
                    msgCtx.getEvalReqStatsServer().setReqReceivedInServiceReplica(System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET);
                    request.deliveryTime = System.currentTimeMillis();

                    MessageContextPair messageContextPair = new MessageContextPair(request, msgCtx);

                    // TODO temporary ByTI hacks
                    //  refactor
                    try {
                        // extract the first action type in the request to distinguish between regular and tick requests
                        ByteArrayInputStream bis = new ByteArrayInputStream(request.getContent());
                        DataInputStream dis = new DataInputStream(bis);
                        EvalActionType actionType = EvalActionType.values()[dis.readInt()];
                        int tickReqCounter = actionType == EvalActionType.ByTI ? dis.readInt() : -1;

                        // tell ByTI about the received request
                        if(evalServer.getByTIManager() != null && evalServer.getByTIManager().isByTIStarted()) {
                            boolean decision = evalServer.getByTIManager().requestReceived(actionType, tickReqCounter
                                    , msgCtx);
                            // if a decision can be made, let the ByTIManager do just that in a new UDS Thread
                            if(decision) {
                                // get current ByTI values
                                int byTIId = evalServer.getByTIManager().getByTIId();
                                int firstNo = evalServer.getByTIManager().getFirstReqInByTINo();
                                int lastNo = evalServer.getByTIManager().getLastReqInByTINo();
                                boolean imprecise = evalServer.getByTIManager().isImprecise();
                                long byTICloseTime = evalServer.getByTIManager().getByTICloseTimeNs();
                                int reqCounter = evalServer.getByTIManager().getDecidedReqCounter();

                                // prepare next interval (reset ByTI values in ByTIManager)
                                evalServer.getByTIManager().prepareNextInterval();

                                // let UDS schedule the decision, for determinism
                                Runnable decisionRunnable = () -> evalServer.getByTIManager()
                                        .decide(byTIId, firstNo, lastNo, reqCounter, imprecise, byTICloseTime);
                                UDScheduler.getInstance().addRequestAndFillRound(decisionRunnable, () -> {});
                            }
                        }
                        // if it's a regular request --> UDS thread
                        if(actionType != EvalActionType.ByTI) {
                            // execute the business logic and get the response bytes
                            FutureTask<byte[]> evalFuture = new FutureTask<>(() -> {
                                // profiling
                                msgCtx.getEvalReqStatsServer().setReqStartedExecution(System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET);

                                return ((SingleExecutable) executor).
                                        executeOrdered(messageContextPair.message.getContent(), messageContextPair.msgCtx);
                            });

                            // the runnable responsible for fulfilling the client request
                            Runnable replyRunnable = () -> {
                                // create the TOMMessage reply from the response bytes
                                try {
                                    messageContextPair.message.reply = new TOMMessage(id, messageContextPair.message.getSession(),
                                            messageContextPair.message.getSequence(), messageContextPair.message.getOperationId(),
                                            evalFuture.get(), SVController.getCurrentViewId(),
                                            TOMMessageType.ORDERED_REQUEST);
                                } catch(InterruptedException | ExecutionException e) {
                                    logger.severe(e.getMessage());
                                    e.printStackTrace();
                                }

                                // send out the reply
                                replier.manageReply(messageContextPair.message, messageContextPair.msgCtx);

                                // profiling
                                msgCtx.getEvalReqStatsServer().setReqFullyCompletedAndSentBackReply(System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET);

                                // add the completed request stats to the server deque so it can be logged to disk
                                // eventually
                                evalServer.getEvalReqStatsServerDeque().addFirst(msgCtx.getEvalReqStatsServer());
                            };
                            // blocking call, give request to scheduler as soon as it accepts new Runnables
                            UDScheduler.getInstance().addRequest(evalFuture, replyRunnable);

                            // TODO temp code to test dummyReqInsertion
                            udsReqCount++;

                            // profiling
                            msgCtx.getEvalReqStatsServer().setReqSubmittedtoUDS(System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET);
                        } else {
                            // it's a ByTI request; just send back an empty reply
                            byte[] byTIReply = new byte[0];
                            messageContextPair.message.reply = new TOMMessage(id,
                                    messageContextPair.message.getSession(),
                                    messageContextPair.message.getSequence(), messageContextPair.message.getOperationId(),
                                    byTIReply, SVController.getCurrentViewId(), TOMMessageType.ORDERED_REQUEST);
                            replier.manageReply(messageContextPair.message, messageContextPair.msgCtx);
                        }

                    } catch(IOException e) {
                        e.printStackTrace();
                    }

                // If the message was a reconfig-message, let SVController know and don't set noop to false
                } else if (request.getViewID() == SVController.getCurrentViewId() &&
                        request.getReqType() == TOMMessageType.RECONFIG) {
                    SVController.enqueueUpdate(request);

                // message sender had an old view; resend the message to him (but only if it came from consensus
                // and not state transfer)
                } else if (request.getViewID() < SVController.getCurrentViewId()) {
                    tomLayer.getCommunication().send(new int[]{request.getSender()}, new TOMMessage(SVController.getStaticConf().getProcessId(),
                    request.getSession(), request.getSequence(), TOMUtil.getBytes(SVController.getCurrentView()), SVController.getCurrentViewId()));

                } else {
                    throw new RuntimeException("Should never reach here!");
                }
                requestCount++;
            }

            // TODO temp code to test dummyReqInsertion
            // insert N - udsReqCount dummyRequests so we can be sure a round is certainly started
            /*
            if(udsReqCount != 0 && udsReqCount < UDScheduler.getInstance().getCurrentUDSConfigurationPrimaries()) {
                for(int i = 0; i < (UDScheduler.getInstance().getCurrentUDSConfigurationPrimaries() - udsReqCount); i++) {
                    UDScheduler.getInstance().addRequest(() -> {}, () -> {});
                }
            }
            */

            // This happens when a consensus finishes but there are no requests to deliver
            // to the application. This can happen if a reconfiguration is issued and is the only
            // operation contained in the batch. The recoverer must be notified about this,
            // hence the invocation of "noop"
            if (noop && this.recoverer != null) {

                //bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Delivering a no-op to the " +
                //        "recoverer");

                System.out.println(" --- A consensus instance finished, but there were no commands to deliver to the application.");
                System.out.println(" --- Notifying recoverable about a blank consensus.");

                byte[][] batch;
                MessageContext[] msgCtx;

                //Make new batch to deliver
                batch = new byte[requestsFromConsensus.length][];
                msgCtx = new MessageContext[requestsFromConsensus.length];

                //Put messages in the batch
                int line = 0;
                for (TOMMessage m : requestsFromConsensus) {
                    batch[line] = m.getContent();

                    msgCtx[line] = new MessageContext(m.getSender(), m.getViewID(),
                            m.getReqType(), m.getSession(), m.getSequence(), m.getOperationId(),
                            m.getReplyServer(), m.serializedMessageSignature, firstRequest.timestamp,
                            m.numOfNonces, m.seed, regencies[consensusCount], leaders[consensusCount],
                            consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, true);
                    msgCtx[line].setLastInBatch();

                    line++;
                }

                this.recoverer.noOp(consId[consensusCount], batch, msgCtx);

                //MessageContext msgCtx = new MessageContext(-1, -1, null, -1, -1, -1, -1, null, // Since it is a noop, there is no need to pass info about the client...
                //        -1, 0, 0, regencies[consensusCount], leaders[consensusCount], consId[consensusCount], cDecs[consensusCount].getConsMessages(), //... but there is still need to pass info about the consensus
                //        null, true); // there is no command that is the first of the batch, since it is a noop
                //msgCtx.setLastInBatch();

                //this.recoverer.noOp(msgCtx.getConsensusId(), msgCtx);
            }

            consensusCount++;
        }

        //
        // if (SVController.hasUpdates()) {
            // TOMMessage reconf = new TOMMessage(0, 0, 0,0, null, 0, TOMMessageType.ORDERED_REQUEST);
            //MessageContextPair m = new MessageContextPair(reconf, null);

            // TODO deal with view updates (?)
            /* LinkedBlockingQueue[] q = this.scheduler.getMapping().getQueues();
            try {
                for (LinkedBlockingQueue q1 : q) {
                    q1.put(m);
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
            */

        //}
    }
}
