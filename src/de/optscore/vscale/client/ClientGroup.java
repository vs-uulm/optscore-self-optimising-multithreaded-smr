package de.optscore.vscale.client;

public interface ClientGroup {

    public void activate();

    public void deactivate();

    public void shutdown();

    public boolean isActive();
}
