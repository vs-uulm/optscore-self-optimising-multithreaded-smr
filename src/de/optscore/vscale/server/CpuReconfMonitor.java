package de.optscore.vscale.server;

import de.optscore.reconfiguration.cpu.CpuReconfigurationException;
import de.optscore.reconfiguration.cpu.CpuReconfigurator;
import de.optscore.vscale.util.BufferedStatsWriter;
import de.optscore.vscale.util.MeanVarianceSampler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class monitors the current load on a replica using various means, like average response times of the last X
 * requests or average CPU load, etc.
 * It can decide to reconfigure CPU cores on the fly if the system is over-/underutilized.
 */
public class CpuReconfMonitor {

    private final CpuReconfigurator cpuReconfigurator;
    private final BufferedStatsWriter statsWriter;

    private final long[] requestWindow;
    private final AtomicInteger index;
    private final MeanVarianceSampler avgResponseTimeNs;

    private final int maxCoreCount = 32;

    public CpuReconfMonitor(CpuReconfigurator cpuReconfigurator, BufferedStatsWriter statsWriter) {
        this(cpuReconfigurator, statsWriter, 1000);
    }

    public CpuReconfMonitor(CpuReconfigurator cpuReconfigurator, BufferedStatsWriter statsWriter, int windowSize) {
        this.cpuReconfigurator = cpuReconfigurator;
        this.statsWriter = statsWriter;
        this.requestWindow = new long[windowSize];
        this.index = new AtomicInteger(0);
        this.avgResponseTimeNs = new MeanVarianceSampler();
        for(int i = 0; i < requestWindow.length; i++) {
            requestWindow[i] = 0l;
            avgResponseTimeNs.add(0d);
        }
    }

    /**
     * Updates the average response time over the last windowSize requests and returns the new average
     *
     * @param reponseTimeNs the time in ns that the last request took to fully process
     * @return the new average of all the last windowSize requests' responseTimes
     */
    public long updateAvgResponseTimeNs(long reponseTimeNs) {
        // get an array spot
        int i = index.getAndIncrement();
        // remember old value to replace in MeanVarianceSampler
        long oldVal = requestWindow[i % requestWindow.length];
        // save the new responseTime (i.e. slide window)
        requestWindow[i % requestWindow.length] = reponseTimeNs;

        // TODO MeanVarianceSampler is NOT threadsafe, but hopefully with a large enough windowSize this doesn't matter?
        avgResponseTimeNs.replace(oldVal, reponseTimeNs);

        //updateConfiguration();

        return (long) avgResponseTimeNs.getMean();
    }

    /**
     * Decides (based on currently available data) whether CPU core count needs to be reconfigured
     */
    private void updateConfiguration() {
        // TODO proper logic
        //  currently: If avgResponseTime > 10ms and coreCount not yet maxCoreCount, increase coreCount by 1
        if(avgResponseTimeNs.getMean() > 15 * 1000000) {
            try {
                int currentCoreCount = cpuReconfigurator.numberOfActiveCpuCores();
                if(currentCoreCount < maxCoreCount) {
                    long currentTime = System.nanoTime() + EvalServer.BENCHMARK_NANOTIME_OFFSET;
                    cpuReconfigurator.addCpuCore();
                    statsWriter.writeCPUReconfigured(currentTime, currentCoreCount);
                    System.out.println("*++ Reconfigured cores from " + currentCoreCount + " cores to new coreCount: " +
                            cpuReconfigurator.numberOfActiveCpuCores() +
                            " (avgResponseTime was " + avgResponseTimeNs.getMean() / 1000000 + "ms)");
                }
            } catch(CpuReconfigurationException e) {
                e.printStackTrace();
            }

        }
    }

}
