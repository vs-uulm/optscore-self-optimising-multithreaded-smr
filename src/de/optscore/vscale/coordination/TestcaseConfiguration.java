package de.optscore.vscale.coordination;

/**
 * Helper object for conveniently storing configuration data for a test case. Since its only purpose is to ease the
 * passing of lots of parameters in a convenient way, no getter/setters for fields.
 */
public class TestcaseConfiguration {

    public int id;

    public String testcaseId;
    public String testcaseDesc;
    public String testcaseGitCommit;
    public int runsCompleted;
    public boolean withUDS;
    public int numOfActiveCores;
    public int udsConfPrim;
    public int udsConfSteps;
    public int numOfReplicas;
    public String[] replicaIPs;
    public int[] replicaPorts;

    /**
     * (optional) For creating new test cases in the DMark DB, to check whether a workload already exists
     */
    public int workloadId;

    public TestcaseConfiguration(String testcaseId) {
        this.testcaseId = testcaseId;
    }

    public void setReplicas(String replicasFromDB) {
        String[] replicaHostStrings = replicasFromDB.split(";");
        this.numOfReplicas = replicaHostStrings.length;
        this.replicaIPs = new String[numOfReplicas];
        this.replicaPorts = new int[numOfReplicas];
        for(int i = 0; i < numOfReplicas; i++) {
            String[] tmp = replicaHostStrings[i].split(":");
            replicaIPs[i] = tmp[0];
            replicaPorts[i] = Integer.parseInt(tmp[1]);
        }
    }

    public String getReplicasAsString() {
        String replicaString = "";
        for(int i = 0; i < numOfReplicas; i++) {
            replicaString += replicaIPs[i];
            replicaString += ":";
            replicaString += replicaPorts[i];
            if(i != numOfReplicas - 1) {
                replicaString += ";";
            }
        }
        return replicaString;
    }

    public String getSummary() {
        return "Configuration parameters for test case " + testcaseId + ":\n" +
                "testcaseDescription: " + testcaseDesc + "\n" +
                "gitCommit: " + testcaseGitCommit + "\n" +
                "runNumber: " + runsCompleted + "\n" +
                "withUDS: " + withUDS + "\n" +
                "udsConfPrim: " + udsConfPrim + "\n" +
                "udsConfSteps: " + udsConfSteps + "\n" +
                "numOfActiveCores: " + numOfActiveCores + "\n" +
                "replicas: " + getReplicasAsString() + "\n";
    }
}
