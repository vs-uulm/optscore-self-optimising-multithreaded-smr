package de.optscore.vscale.client;

import com.lmax.disruptor.dsl.Disruptor;
import de.optscore.vscale.RequestProfile;
import de.optscore.vscale.util.EvalReqStatsClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class ClientGroupInstance implements ClientGroup {

    private static final Logger logger = Logger.getLogger(ClientGroupInstance.class.getName());

    private int clientGroupId;
    private int numOfClients;
    private RequestProfile requestProfile;
    private long sendDelayNs;

    private final ClientGroupManager clientGroupManager;

    private final EvalClient[] clients;
    private final ReentrantLock activationLock;
    private final Condition activationCondition;

    // used to determine whether this group is currently active (i.e. clients have been signalled to wake up)
    private boolean active;

    private Disruptor<EvalReqStatsClient> loggingDisruptor;

    private ExecutorService threadpool;

    public ClientGroupInstance(int clientGroupId, int numOfClients,
                               RequestProfile requestProfile, long sendDelayNs, ClientGroupManager clientGroupManager,
                               Disruptor<EvalReqStatsClient> loggingDisruptor) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.clientGroupId = clientGroupId;
        this.numOfClients = numOfClients;
        this.requestProfile = requestProfile;
        this.sendDelayNs = sendDelayNs;
        this.clientGroupManager = clientGroupManager;

        this.activationLock = new ReentrantLock();
        this.activationCondition = activationLock.newCondition();
        this.active = false;

        this.loggingDisruptor = loggingDisruptor;

        this.threadpool = Executors.newCachedThreadPool();
        this.clients = new EvalClient[numOfClients];
        this.createClients();
    }

    /**
     * Create all clients, which immediately connects them to the replicas
     */
    private void createClients() {
        try {
            for(int i = 0; i < numOfClients; i++) {
                clients[i] = new SynchronousEvalClient(clientGroupManager.getNextPid(),
                        requestProfile,
                        sendDelayNs,
                        clientGroupManager.getRequestProfileRepository(),
                        activationLock, activationCondition, loggingDisruptor);
                // immediately start clients (clients are runnables); they will immediately wait until woken up
                threadpool.submit(clients[i]);

                // don't overload the replicas by establishing too many connections at once
                if(i % 5 == 0) {
                    Thread.sleep(50);
                }

            }
        } catch(IndexOutOfBoundsException e) {
            logger.warning("Could not instantiate client! " + e.getMessage());
            e.printStackTrace();
            logger.severe("!! Test case inconsistent: ABORTING !!");
            System.exit(1);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Instruct all clients of this group to start sending the configured requests with the configured delay for this
     * group. First set their signal flag to true (workaround against spurious wakeups), then notify all clients.
     */
    public void activate() {
        this.active = true;
        // first set the condition on all clients ...
        for(EvalClient client : clients) {
            client.wakeUpClient();
        }
        // ... then wake them up
        activationLock.lock();
        try {
            activationCondition.signalAll();
        } finally {
            activationLock.unlock();
        }

    }

    /**
     * Stops all clients in this group. They will stay connected, but stop sending requests (while still receiving
     * replies and logging times for open requests)
     */
    public void deactivate() {
        this.active = false;
        for(EvalClient client : clients) {
            client.putClientToSleep();
        }
    }

    public boolean isActive() {
        return active;
    }

    public int getClientGroupId() {
        return clientGroupId;
    }

    public int getNumOfClients() {
        return numOfClients;
    }

    public RequestProfile getRequestProfile() {
        return requestProfile;
    }

    public long getSendDelayNs() {
        return sendDelayNs;
    }

    public void shutdown() {
        this.active = false;
        // call shutdownClient() to set their internal clientClosed flag ...
        for(EvalClient client : clients) {
            client.shutdownClient();
        }
        // ... then activate the clients one last time, so the client threads can exit the loop and end
        activationLock.lock();
        try {
            activationCondition.signalAll();
        } finally {
            activationLock.unlock();
        }
    }
}
