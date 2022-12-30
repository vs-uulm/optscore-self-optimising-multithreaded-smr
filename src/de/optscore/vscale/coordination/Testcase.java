package de.optscore.vscale.coordination;

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A testcase consists of
 * - a id which is a hash of all its following parts
 * - a {@link TestcaseConfiguration}, which specifies static parameters that stay the same during this testrun
 * - a {@link WorkloadPlaybook}, which contains the {@link PlaybookAction}s for instructing clientMachines so they
 * start/stop sending requests at certain times during the testcase
 */
public class Testcase {

    private String id;

    private final TestcaseConfiguration testcaseConfiguration;

    private final WorkloadPlaybook workloadPlaybook;

    private final String description;

    public Testcase(TestcaseConfiguration conf, WorkloadPlaybook playbook, String description) {
        this.testcaseConfiguration = conf;
        this.workloadPlaybook = playbook;
        this.description = description;
        this.id = hashTestcase();
    }

    public String getId() {
        if(id == null) {
            updateId();
        }
        return id;
    }

    /**
     * Recalculate the id of this test case (which is a hash of its parts, see hashTestcase method)
     */
    private void updateId() {
        this.id = hashTestcase();
    }

    public TestcaseConfiguration getTestcaseConfiguration() {
        return testcaseConfiguration;
    }

    public WorkloadPlaybook getWorkloadPlaybook() {
        return workloadPlaybook;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Creates a summary of all this testcases configuration parameters and playbook actions for documentation purposes
     * @return A string containing all details of this testcase, which can for example direcly be saved to a file
     */
    public String getTestcaseSummary() {
        return "Summary for test case " + id + ":\n" +
                testcaseConfiguration.getSummary() + "\n" +
                workloadPlaybook.serializePlaybook();
    }

    /**
     * Returns a hash for this testcase, by hashing all of its configuration and workloadPlaybook entries
     * This hash in a Hex representation is used as the testcaseId and will be automatically set when this method runs
     * @return A hash of the testcase, which is also its Id used to reference it in the DB
     */
    public String hashTestcase() {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");

            /*
                included in a testcase hash:
                - String testcaseGitCommit;
                - boolean withUDS;
                - int numOfActiveCores;
                - int udsConfPrim;
                - int udsConfSteps;
                - String with replicaIPs:Ports;
                - all workloadPlaybookActions, each including
                    - String clientMachineIP;
                    - int clientMachineId;
                    - int addRemoveModifier;
                    - int clientGroupId;
                    - int clientGroupNumOfClients;
                    - int clientGroupRequestProfile;
                    - long clientGroupSendDelay;
                    - long timeOffsetNs;
             */

            // first the test case configuration
            TestcaseConfiguration conf = this.getTestcaseConfiguration();
            sha.update(conf.testcaseGitCommit.getBytes(StandardCharsets.UTF_8));
            sha.update((byte) (conf.withUDS ? 1 : 0));
            // ByteBuffer for the 3 ints numOfActiveCores, udsConfPrim, udsConfSteps
            ByteBuffer confBuffer = ByteBuffer.allocate(3 * 4);
            confBuffer.putInt(conf.numOfActiveCores);
            confBuffer.putInt(conf.udsConfPrim);
            confBuffer.putInt(conf.udsConfSteps);
            confBuffer.flip();
            sha.update(confBuffer);
            sha.update(conf.getReplicasAsString().getBytes(StandardCharsets.UTF_8));

            // and each playbook item; ByteBuffer for the 5 ints and 2 longs
            final ByteBuffer playbookBuffer = ByteBuffer.allocate(5 * 4 + 2 * 8);
            this.getWorkloadPlaybook().getPlaybookActions().forEach(playbookAction -> {
                playbookBuffer.putInt(playbookAction.getClientMachine().getId());
                playbookBuffer.putInt(playbookAction.getAddRemoveModifier());
                playbookBuffer.putInt(playbookAction.getClientGroup().getId());
                playbookBuffer.putInt(playbookAction.getClientGroup().getNumOfClients());
                playbookBuffer.putInt(playbookAction.getClientGroup().getRequestProfile().getProfileId());
                playbookBuffer.putLong(playbookAction.getClientGroup().getSendDelayNs());
                playbookBuffer.putLong(playbookAction.getTimeOffsetNs());
                playbookBuffer.flip();
                sha.update(playbookAction.getClientMachine().getIP().getBytes(StandardCharsets.UTF_8));
                sha.update(playbookBuffer);
                playbookBuffer.clear();
            });
            byte[] hash = sha.digest();
            this.id = DatatypeConverter.printHexBinary(hash);
            return this.id;
        } catch(NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}
