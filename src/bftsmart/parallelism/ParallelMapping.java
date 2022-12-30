/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author alchieri
 */
public class ParallelMapping {

    public static int CONFLICT_NONE = -1;
    public static int CONFLICT_ALL = -2;
    public static int CONFLICT_RECONFIGURATION = -3;

    private Map<Integer, LinkedBlockingQueue[]> groups = new HashMap<Integer, LinkedBlockingQueue[]>();
    private Map<Integer, CyclicBarrier> barriers = new HashMap<Integer, CyclicBarrier>();
    private Map<Integer, Integer> executorThread = new HashMap<Integer, Integer>();

    private LinkedBlockingQueue[] queues;
    private CyclicBarrier reconfBarrier;

    private CyclicBarrier reconfThreadBarrier;

    private int numberOfthreadsAC = 0;

    //alex INICIO
    private int maxNumberOfthreads = 0;
    private int minNumberOfThreads = 0;
    public static int THREADS_RECONFIGURATION = -4;
    //alex FIM

    public ParallelMapping(int minNumberOfThreads , int initialNumberOfThreads, int maxNumberOfThreads) {
        //alex
        this.maxNumberOfthreads = maxNumberOfThreads;
        this.minNumberOfThreads = minNumberOfThreads;
        this.numberOfthreadsAC = initialNumberOfThreads;
        //System.out.println("Iniciou com numero = " + this.numberOfthreadsAC + " de threads ativas");
        /*queues = new LinkedBlockingQueue[numThreads];
        for (int i = 0; i < queues.length; i++) {
            queues[i] = new LinkedBlockingQueue();
        }*/
        //FIZ PARA CASO O MAXIMO SEJA DIFERENTE DO NUMERO DE ENTRADA
        queues = new LinkedBlockingQueue[maxNumberOfthreads];
        //queues = new LinkedBlockingQueue[numberOfthreads];
        for (int i = 0; i < queues.length; i++) {
            queues[i] = new LinkedBlockingQueue();
        }       

        this.barriers.put(CONFLICT_ALL, new CyclicBarrier(getNumThreadsAC()));

        this.executorThread.put(CONFLICT_ALL, 0);
        reconfBarrier = new CyclicBarrier(getNumThreadsAC() + 1);
        //reconfThreadBarrier = new CyclicBarrier(numThreads);
        reconfThreadBarrier = new CyclicBarrier(maxNumberOfthreads);
    }

    public CyclicBarrier getBarrier(int groupID) {
        return barriers.get(groupID);
    }

    public int getExecutorThread(int groupId) {
        return executorThread.get(groupId);
    }

    public CyclicBarrier getReconfBarrier() {
        return reconfBarrier;
    }

//Alex inicio 
    public CyclicBarrier getReconfThreadBarrier() {
        return reconfThreadBarrier;
    }

    public int checkNumReconfigurationThreads(int nt) {
        //menor ou igual a minimo retorna minimo
        int numTResult = this.numberOfthreadsAC + nt;
        if (numTResult >= this.minNumberOfThreads && numTResult <= this.maxNumberOfthreads){
            return nt;
        }else{
            if (nt < 0) {
                while (numTResult < this.minNumberOfThreads) {
                    nt++;
                    numTResult = this.numberOfthreadsAC + nt;
                }
            } else if (nt > 0) {
                while (numTResult > this.maxNumberOfthreads) {
                    nt--;
                    numTResult = this.numberOfthreadsAC + nt;
                }
            }
            return nt;
        }
    }

    public void setNumThreadsAC(int x) {
        this.numberOfthreadsAC = x;
    }

    public int getNumMaxOfThreads() {
        return this.maxNumberOfthreads;
    }

    public int getNumMinOfThreads() {
        return this.minNumberOfThreads;
    }

    public LinkedBlockingQueue[] getQueuesActive() {
        LinkedBlockingQueue[] qAtivas = new LinkedBlockingQueue[getNumThreadsAC()];
        for (int i = 0; i < qAtivas.length; i++) {
            qAtivas[i] = queues[i];
        }
        return qAtivas;
    }

    //Alex fim
    public void reconfigureBarrier() {
        
        this.barriers.remove(CONFLICT_ALL);
        this.barriers.put(CONFLICT_ALL, new CyclicBarrier(getNumThreadsAC()));
        reconfBarrier = new CyclicBarrier(getNumThreadsAC() + 1);
    }

    public boolean addMultiGroup(int groupId, int[] groupsId) {
        if (groupId >= getNumThreadsAC()) {

            LinkedBlockingQueue[] q = new LinkedBlockingQueue[groupsId.length];
            for (int i = 0; i < q.length; i++) {
                q[i] = queues[groupsId[i]];
                System.out.println("GID: " + groupId + " m:" + groupsId[i]);
            }
            groups.put(groupId, q);
            this.barriers.put(groupId, new CyclicBarrier(groupsId.length));
            this.executorThread.put(groupId, groupsId[0]);
            return true;
        }
        return false;
    }

    public LinkedBlockingQueue[] getMultiGroup(int groupId) {
        return groups.get(groupId);
    }

    public LinkedBlockingQueue[] getQueues() {
        return queues;
    }

    public int getNumThreadsAC() {
        return this.numberOfthreadsAC;
    }

    public LinkedBlockingQueue getThreadQueue(int threadID) {
        return queues[threadID];
    }

    public LinkedBlockingQueue[] getQueues(int groupID) {
        if (groupID == CONFLICT_NONE) {
            LinkedBlockingQueue[] r = {queues[0]};
            return r;
        } else if (groupID == CONFLICT_ALL) {
            return queues;
        } else if (groupID < getNumThreadsAC()) {
            LinkedBlockingQueue[] r = {queues[groupID]};
            return r;
        } else {
            return getMultiGroup(groupID);
        }
    }

}
