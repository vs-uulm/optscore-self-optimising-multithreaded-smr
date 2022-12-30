package de.optscore.vscale.client;

public interface EvalClient extends Runnable {

    void wakeUpClient();

    void putClientToSleep();

    void shutdownClient();

}
