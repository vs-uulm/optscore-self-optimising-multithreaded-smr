package de.optscore.vscale.server.byti;

import bftsmart.communication.client.ReplyListener;
import bftsmart.reconfiguration.ClientViewController;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.EvalRequest;

import java.util.concurrent.TimeUnit;

/**
 * An asynchronous Client for sending a constant-rate (by local wallclock) byti to all other replicas,
 * to implement a BFT cluster-wide clock mechanism, e.g. for reconfiguration monitoring
 */
public class ByTIClient implements Runnable {

    private final AsynchServiceProxy serviceProxy;
    private final ByTIManager byTIManager;

    private boolean malicious;

    public ByTIClient(int processId, ByTIManager byTIManager, boolean malicious) {
        this.serviceProxy = new AsynchServiceProxy(processId);
        this.byTIManager = byTIManager;
        this.malicious = malicious;
    }

    @Override
    public void run() {
        // send a tick request to all replicas, including the count of requests received since the last tick was sent
        this.serviceProxy.invokeAsynchRequest(EvalRequest.serializeEvalRequest(new EvalRequest.EvalRequestBuilder().
                action(EvalActionType.ByTI.getActionTypeCode(), byTIManager.getLocalReqCounter()).
                build()), new BlReplyListener(), TOMMessageType.ORDERED_REQUEST);
        // reset reqCounter in byTIManager for the next tick period
        byTIManager.resetReqCounter();

        // simulate a malicious replica which just spams ticks
        if(malicious) {
            byTIManager.getExecutorService().schedule(this, byTIManager.getCurrentByTITickrateMs() / 50,
                    TimeUnit.MILLISECONDS);
        // normal replica, wait tickrate ms before sending the next tick
        } else {
            byTIManager.getExecutorService().schedule(this, byTIManager.getCurrentByTITickrateMs(), TimeUnit.MILLISECONDS);
        }
    }

    protected ClientViewController getViewManager() {
        return serviceProxy.getViewManager();
    }

    /**
     * Copied from {@link bftsmart.tom.ServiceProxy}
     * @return The currently needed quorum, depending on active replicas and number of faults tolerated
     */
    protected int getReplyQuorum() {
        if (serviceProxy.getViewManager().getStaticConf().isBFT()) {
            return (int) Math.ceil((serviceProxy.getViewManager().getCurrentViewN()
                    + serviceProxy.getViewManager().getCurrentViewF()) / 2) + 1;
        } else {
            return (int) Math.ceil((serviceProxy.getViewManager().getCurrentViewN()) / 2) + 1;
        }
    }

    protected int getCurrentViewF() {
        return serviceProxy.getViewManager().getCurrentViewF();
    }

    private class BlReplyListener implements ReplyListener {
        private int replies = 0;

        @Override
        public void reset() {
            replies = 0;
        }

        @Override
        public void replyReceived(RequestContext context, TOMMessage reply) {
            // calculate quorum
            double q = Math.ceil((double) (serviceProxy.getViewManager().getCurrentViewN() + serviceProxy.getViewManager().getCurrentViewF() + 1) / 2.0);
            // if enough replies have been received, clean the request
            if (replies >= q) {
                serviceProxy.cleanAsynchRequest(context.getOperationId());
            }
        }
    }
}