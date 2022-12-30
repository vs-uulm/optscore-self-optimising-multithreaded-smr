package de.optscore.vscale.util;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class BufferedStatsWriter {

    private BufferedWriter writer;
    private static final String DEFAULT_SEPARATOR = ",";

    public BufferedStatsWriter(String fileName, String[] headers) {
        // initialize the writer
        try {
            this.writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8);
            // write the first line with column names
            for(int i = 0; i < headers.length; i++) {
                writer.write(headers[i] + ((i == headers.length - 1) ? "" : DEFAULT_SEPARATOR));
            }
            writer.newLine();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes a line with all details about a single request to a file via a BufferedWriter for good-ish performance.
     * @param reqStatsClient The object containing all details about the request (clientPid, opId, times)
     */
    public void writeEvalReqStatsClient(EvalReqStatsClient reqStatsClient) {
        String line = reqStatsClient.getClientPid() + DEFAULT_SEPARATOR
                + reqStatsClient.getOpId() + DEFAULT_SEPARATOR
                + reqStatsClient.getSentTime() + DEFAULT_SEPARATOR
                + reqStatsClient.getReceivedTime();

        try {
            writer.write(line);
            writer.newLine();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes a line with all details about a single request to a file via a BufferedWriter for good-ish performance.
     * @param reqStatsServer The object containing all details about the request
     */
    public void writeEvalReqStatsServer(long currentTimeNs, EvalReqStatsServer reqStatsServer) {
        String line = currentTimeNs + DEFAULT_SEPARATOR
                + reqStatsServer.getReqReceivedInServiceReplica() + DEFAULT_SEPARATOR
                + reqStatsServer.getReqSubmittedtoUDS() + DEFAULT_SEPARATOR
                + reqStatsServer.getReqStartedExecution() + DEFAULT_SEPARATOR
                + reqStatsServer.getReqEndedExecution() + DEFAULT_SEPARATOR
                + reqStatsServer.getReqFullyCompletedAndSentBackReply();
        try {
            writer.write(line);
            writer.newLine();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void writeCPUReconfigured(long currentTimeNs, int currentCoreCount) {
        String line = currentTimeNs + DEFAULT_SEPARATOR
                + currentCoreCount;
        try {
            writer.write(line);
            writer.newLine();
            // TODO remove flush, proper closing of writer when replica closes/shuts down
            writer.flush();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void writeByTIClosed(long currentTimeNs, int byTIId,
                                int firstNo, int lastNo, int reqCounter, int currentPrimaries,
                                boolean imprecise,
                                long byTICloseTime) {
        String line = currentTimeNs + DEFAULT_SEPARATOR
                + byTIId + DEFAULT_SEPARATOR
                + firstNo + DEFAULT_SEPARATOR
                + lastNo + DEFAULT_SEPARATOR
                + reqCounter + DEFAULT_SEPARATOR
                + currentPrimaries + DEFAULT_SEPARATOR
                + imprecise + DEFAULT_SEPARATOR
                + byTICloseTime;
        try {
            writer.write(line);
            writer.newLine();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void writeByTITick(long currentTimeNs, int senderId, int globalReqSequence,
                              int tickReqCounter, int consensusId) {
        String line = currentTimeNs + DEFAULT_SEPARATOR
                + senderId + DEFAULT_SEPARATOR
                + globalReqSequence + DEFAULT_SEPARATOR
                + tickReqCounter + DEFAULT_SEPARATOR
                + consensusId;
        try {
            writer.write(line);
            writer.newLine();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void flush() {
        try {
            this.writer.flush();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            this.writer.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

}
