package de.optscore.vscale.server;

public interface AutoScaler {
    public void decideScaling(int byTIId, int reqCounter, boolean imprecise);
}
