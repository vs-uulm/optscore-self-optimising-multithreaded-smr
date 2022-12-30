package de.optscore.vscale.util.cli;

import de.optscore.vscale.RequestProfile;
import de.optscore.vscale.coordination.*;
import org.beryx.textio.TextIO;
import org.beryx.textio.TextIoFactory;
import org.beryx.textio.TextTerminal;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TestcaseCreator {

    private final TextIO textIO;
    private final TextTerminal terminal;

    private final DMarkAdapter db;

    /**
     * The id of the workload that is to be used for creating a new test case. Can be newly created or pre-existing,
     * both cases will be handled by the appropriate methods.
     */
    private int workloadId;

    public TestcaseCreator(String dbPath) {
        this.textIO = TextIoFactory.getTextIO();
        this.terminal = textIO.getTextTerminal();
        this.db = new DMarkAdapter(dbPath);

        // disable autoCommit in the DB, so we can roll back the transaction when something goes awry
        try {
            db.setAutoCommit(false);
        } catch(SQLException e) {
            e.printStackTrace();
            System.err.println("Could not disable autoCommit in the DB. Aborting");
            System.exit(2);
        }
    }

    /**
     * Determine whether the user wants to re-use an existing workload (i.e. only modify configuration for a new test
     * case) or create a new workload. The workloadId will be saved in the field workloadId
     * @return true if a new workload was created, false if a pre-existing workload was chosen
     */
    private boolean determineNewOrExistingWorkload() {
        List<String> workloads = db.queryWorkloads();
        workloads.add(0, "0 | [Create a new workload]");

        String chosenWorkload = textIO.newStringInputReader()
                .withNumberedPossibleValues(workloads)
                .read("Choose an existing workload or create a new one");
        Pattern pattern = Pattern.compile("(\\d+).*");
        Matcher matcher = pattern.matcher(chosenWorkload);
        if(matcher.find()) {
            int chosenWorkloadId = Integer.parseInt(matcher.group(1));
            if(chosenWorkloadId == 0) {
                String newWorkloadDescription = textIO.newStringInputReader().read("Describe the new workload");
                this.workloadId = db.insertNewWorkload(newWorkloadDescription);
                terminal.println("A new workload was created (workloadId: " + workloadId + ")");
                return true;
            }
            matcher = pattern.matcher(workloads.get(chosenWorkloadId));
            if(matcher.find()) {
                this.workloadId = Integer.parseInt(matcher.group(1));
                terminal.println("A pre-existing workload was chosen (workloadId: " + workloadId + ")");
                return false;
            }
        }
        return false;
    }

    private WorkloadPlaybook queryExistingWorkloadPlaybook() {
        WorkloadPlaybook playbook = db.queryPlaybookActionsForWorkload(workloadId);

        if(!playbook.isValid()) {
            terminal.println("ERROR: The workload queried from the database was invalid. This means the DB is somehow" +
                    " inconsistent. Please manually check what's wrong. Rolling back ...");
            try {
                db.rollbackTransaction();
                db.setAutoCommit(true);
            } catch(SQLException e) {
                e.printStackTrace();
                System.err.println("ERROR: Could not rollback the transaction. This probably means the DB is " +
                        "inconsistent now. Please manually check the DB. Aborting.");
                System.exit(4);
            }
            System.exit(5);
        }

        return playbook;
    }

    /**
     * When creating a new workload, this prompts the user to select clientMachines from a list or create new ones
     * @return a list of clientMachines (after this method they will exist in the DB if they didn't before)
     */
    private List<DBClientMachine> determineClientMachines() {
        int numOfClientMachines = textIO.newIntInputReader()
                .withMinVal(1)
                .read("\nHow many Client Machines do you want to use for the new workload?");


        List<DBClientMachine> clientMachines = db.queryClientMachines();
        clientMachines.add(0, new DBClientMachine(0, "0.0.0.0", 0, "[Create a new machine]"));
        List<DBClientMachine> chosenMachines = new LinkedList<>();

        for(int i = 0; i < numOfClientMachines; i++) {
            terminal.println("Choice #" + (i + 1) + ": Choose a client machine from an existing one or create a " +
                    "new machine.");

            DBClientMachine chosenMachine = textIO.<DBClientMachine>newGenericInputReader(null)
                            .withNumberedPossibleValues(clientMachines)
                            .read("Choose client machine");

            if(chosenMachine.getId() == 0) {
                // create a new ClientMachine in the DB
                String newMachineIP = textIO.newStringInputReader()
                        .withPattern("\\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}" +
                                "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\b")
                        .read("Enter the IP address of the new client machine");
                int newMachinePort = textIO.newIntInputReader()
                        .withMinVal(1)
                        .withMaxVal(65536)
                        .read("Enter the port of the new client machine");
                String newMachineDescription = textIO.newStringInputReader()
                        .read("Enter a description for the new client machine");

                DBClientMachine newClientMachine = new DBClientMachine();
                newClientMachine.setIP(newMachineIP);
                newClientMachine.setPort(newMachinePort);
                newClientMachine.setDescription(newMachineDescription);
                // save the new client machine to the DB and get its id
                newClientMachine.setId(db.insertNewClientMachine(newClientMachine));
                chosenMachines.add(newClientMachine);
                terminal.println("The new machine (" + newClientMachine.toString() + ") has been added to the DB.");
            } else {
                chosenMachines.add(chosenMachine);
            }

            terminal.println(chosenMachines.stream()
                    .map(DBClientMachine::toString)
                    .collect(Collectors.joining("\n", "Chosen client machines so far:\n", "\n")));
        }

        terminal.println(chosenMachines.stream()
                .map(DBClientMachine::toString)
                .collect(Collectors.joining("\n", "=== Final list of client machines:\n", "\n")));
        return chosenMachines;
    }

    /**
     * When creating a new workload, this prompts the user to select clientGroups from a list or create new ones
     *
     */
    private List<DBClientGroup> determineClientGroups() {
        int numOfClientGroups = textIO.newIntInputReader()
                .withMinVal(1)
                .read("\nHow many different kinds of ClientGroups do you need for the workload?");


        List<DBClientGroup> clientGroups = db.queryClientGroups();
        clientGroups.add(0, new DBClientGroup(0,
                0,
                RequestProfile.NOOP,
                0,
                "[Create a new ClientGroup]"));
        List<DBClientGroup> chosenClientGroups = new LinkedList<>();

        for(int i = 0; i < numOfClientGroups; i++) {
            terminal.println("Choice #" + i + ": Choose a client group from an existing one or create a new group.");

            DBClientGroup chosenGroup = textIO.<DBClientGroup>newGenericInputReader(null)
                    .withNumberedPossibleValues(clientGroups)
                    .read("Choose client group:");

            if(chosenGroup.getId() == 0) {
                // create a nee ClientGroup in the DB

                int numOfClients = textIO.newIntInputReader()
                        .withMinVal(1)
                        .read("Specify the number of clients the client group shall have");
                RequestProfile profile = textIO.newEnumInputReader(RequestProfile.class)
                        .read("Choose request profile all clients of this group will send");
                int sendDelayMs = textIO.newIntInputReader()
                        .withMinVal(0)
                        .read("Specify how long each client should wait between receiving a reply and sending a new " +
                                "request (in milliseconds!)");
                String description = textIO.newStringInputReader()
                        .read("Describe the new client group as best as possible");

                DBClientGroup newClientGroup = new DBClientGroup(0,
                        numOfClients,
                        profile,
                        sendDelayMs * 1000000L,
                        description);
                // save the new client group to DB and get its id
                newClientGroup.setId(db.insertNewClientGroup(newClientGroup));
                chosenClientGroups.add(newClientGroup);
                terminal.println("The new client group (" + newClientGroup.toString() + ") has been added to the DB.");
            } else {
                chosenClientGroups.add(chosenGroup);
            }

            terminal.println(chosenClientGroups.stream()
                    .map(DBClientGroup::toString)
                    .collect(Collectors.joining("\n", "Chosen client groups so far:\n", "\n")));
        }

        terminal.println(chosenClientGroups.stream()
                .map(DBClientGroup::toString)
                .collect(Collectors.joining("\n", "=== Final list of client groups:\n", "\n")));
        return chosenClientGroups;
    }


    private WorkloadPlaybook determineWorkloadPlaybook(List<DBClientMachine> availableClientMachines,
                                                       List<DBClientGroup> availableClientGroups) {
        WorkloadPlaybook playbook = new WorkloadPlaybook("not yet determined");

        terminal.println("\nYou can now create a new playbook for this workload by adding playbook actions.");

        // keep adding actions until the user chooses to stop
        boolean playbookFinished = false;
        while(!playbookFinished) {
            playbook.addPlaybookAction(determinePlaybookAction(availableClientMachines, availableClientGroups));

            terminal.println(playbook.getPlaybookActions().stream()
                    .map(PlaybookAction::toString)
                    .collect(Collectors.joining("\n", "Actions in the playbook so far:\n", "\n==================\n")));
            playbookFinished = !textIO.newBooleanInputReader()
                    .read("Add another action to the playbook? (playbook has to be valid when choosing 'no'!)");

            if(playbookFinished) {
                // check validity of playbook
                if(!playbook.isValid()) {
                    terminal.println("Playbook is not valid! Please check the playbook actions and make sure every " +
                            "added client group instance is also removed at some point!");
                    // maybe the user wants to fix the mistake(s)
                    playbookFinished = !textIO.newBooleanInputReader()
                            .read("\nDo you want to continue entering actions to fix the playbook?");
                }
            }
        }

        if(!playbook.isValid()) {
            terminal.println("\n=== ERROR: The created playbook is invalid! Previously created client machines and " +
                    "client groups will be rolled back, since the playbook is not internally consistent (e.g. not all" +
                    " started clientGroups are removed until the test ends), so nothing will be saved. Please try " +
                    "creating a new test case by restarting the TestcaseCreator. ===\n");
            try {
                db.rollbackTransaction();
                db.setAutoCommit(true);
            } catch(SQLException e) {
                e.printStackTrace();
                System.err.println("ERROR: Could not rollback the transaction. This probably means the DB is " +
                        "inconsistent now. Please manually check the DB. Aborting.");
                System.exit(4);
            }
            System.exit(1);
        }

        System.out.println(playbook.getPlaybookActions().stream()
                .map(PlaybookAction::toString)
                .collect(Collectors.joining("\n", "=== Actions in the final playbook:\n", "\n================\n")));

        return playbook;
    }

    private PlaybookAction determinePlaybookAction(List<DBClientMachine> availableClientMachines,
                                                   List<DBClientGroup> availableClientGroups) {
        PlaybookAction action = new PlaybookAction();

        terminal.println("\n=== Creating a new action for the playbook:");

        int timeOffsetMs = textIO.newIntInputReader()
                .withMinVal(0)
                .read("\nSpecify the time offset from the beginning of the test case for this action (in ms)");
        DBClientGroup clientGroup = textIO.<DBClientGroup>newGenericInputReader(null)
                .withNumberedPossibleValues(availableClientGroups)
                .read("Choose the client group (i.e. which requests to send, how many clients, ...) for this action");
        int addRemoveModifier = textIO.newIntInputReader()
                .read("Specify how many of the chosen client groups should be added(+)/removed(-) with this action " +
                        "(e.g. '3' to add 3 instances, or '-2' to remove 2)");
        DBClientMachine clientMachine = textIO.<DBClientMachine>newGenericInputReader(null)
                .withNumberedPossibleValues(availableClientMachines)
                .read("Choose which client machine should instantiate the chosen client groups");

        action.setTimeOffsetNs(timeOffsetMs * 1000000L);
        action.setAddRemoveModifier(addRemoveModifier);
        action.setClientGroup(clientGroup);
        action.setClientMachine(clientMachine);

        // insert the new action in the
        db.insertNewPlaybookAction(action, workloadId);

        terminal.println("\nNew action created: " + action.toString() + "\n");

        return action;
    }

    private TestcaseConfiguration determineTestcaseConfiguration() {
        //    public String testcaseGitCommit;
        //    public boolean withUDS;
        //    public int numOfActiveCores;
        //    public int udsConfPrim;
        //    public int udsConfSteps;
        //    public String replicas;

        TestcaseConfiguration config = new TestcaseConfiguration("not yet determined");

        config.testcaseGitCommit = textIO.newStringInputReader()
                .withPattern("\\b[0-9a-f]{7,40}\\b")
                .withMinLength(7)
                .withMaxLength(40)
                .read("Provide the current git commit hash (min. length of 7 characters) of the repo under test");
        config.withUDS = textIO.newBooleanInputReader()
                .withTrueInput("On")
                .withFalseInput("Off")
                .read("Test with UDS \"On\" or \"Off\"?");
        config.numOfActiveCores = textIO.newIntInputReader()
                .withMinVal(1)
                .withMaxVal(64)
                .read("Specify how many cores should be active on the replicas");
        if(config.withUDS) {
            config.udsConfPrim = textIO.newIntInputReader()
                    .withMinVal(1)
                    .read("Specify the number of primaries to be used in the UDS configuration");
            config.udsConfSteps = textIO.newIntInputReader()
                    .withMinVal(1)
                    .read("Specify the number of steps to be used in the UDS configuration");
        }
        List<String> replicas = new LinkedList<>(db.queryReplicaConfigurations());
        replicas.add("[Create new replica configuration]");

        String chosenReplicaConfig = textIO.newStringInputReader()
                .withNumberedPossibleValues(replicas)
                .read("Choose an existing replica configuration or create a new one");
        if(chosenReplicaConfig.substring(0, 1).equals("[")) {
            chosenReplicaConfig = textIO.newStringInputReader()
                    .withPattern("\\b(?:(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}" +
                            "(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):(?:(\\d{1,4}));){3}" +
                            "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" +
                            ":(?:(\\d{1,4}))\\b")
                    .read("Specify a new set of replicas, in the format \"<IPv4>:<port>;<IPv4>:<port>;...\"");
        }
        config.setReplicas(chosenReplicaConfig);
        config.workloadId = workloadId;

        // insert the new test case configuration into the DB
        config.id = db.insertNewTestcaseConfiguration(config);

        terminal.println("\n=== Created a new test case configuration (id " + config.id + "), almost done now ...\n");

        return config;
    }

    private int createTestcase(TestcaseConfiguration testcaseConfig, WorkloadPlaybook playbook) {
        String description = textIO.newStringInputReader()
                .read("\nPlease enter a detailed description of the test case");

        terminal.println("=== All data required for the new test case is now available. Inserting into DB ...");

        Testcase testcase = new Testcase(testcaseConfig, playbook, description);

        // insert the finished test case into the DB
        int testcaseId = db.insertNewTestcase(testcase);

        if(testcaseId == -1) {
            terminal.println("ERROR: Could not save the testcase. Rolling back ...");
            try {
                db.rollbackTransaction();
                db.setAutoCommit(true);
            } catch(SQLException e) {
                e.printStackTrace();
                System.err.println("ERROR: Could not rollback the transaction. This probably means the DB is " +
                        "inconsistent now. Please manually check the DB. Aborting.");
                System.exit(4);
            }
            System.exit(6);
        }

        return testcaseId;
    }

    private void saveTestcaseToDB() {
        try {
            db.commitTransaction();
        } catch(SQLException e) {
            e.printStackTrace();
            System.err.println("ERROR: Could not commit changes to the DB! This probably means nothing was saved. " +
                    "Please check the DB manually! Aborting...");
            System.exit(3);
        }
        terminal.println("\nSuccessfully saved the test case to the database. Congratulations.");
    }

    public void end() {
        // restoring autoCommit in the DB
        try {
            db.setAutoCommit(true);
        } catch(SQLException e) {
            e.printStackTrace();
            System.err.println("ERROR: Could not restore autoCommit in the DB! Please manually check for consistency!");
        }

        textIO.newStringInputReader().withMinLength(0).read("\nPress enter to terminate...");
        textIO.dispose();
    }


    public static void main(String[] args) {
        TestcaseCreator creator = new TestcaseCreator("test-case.db");
        WorkloadPlaybook playbook;

        // ask whether new workload should be created or old one be used
        if(creator.determineNewOrExistingWorkload()) {
            // a new workload was created
            // Choose clientMachines for this workload
            List<DBClientMachine> chosenMachines = creator.determineClientMachines();
            List<DBClientGroup> chosenGroups = creator.determineClientGroups();

            // create a new playbook
            playbook = creator.determineWorkloadPlaybook(chosenMachines, chosenGroups);
        } else {
            playbook = creator.queryExistingWorkloadPlaybook();
        }

        // create a new configuration with the given workload
        TestcaseConfiguration testcaseConfig = creator.determineTestcaseConfiguration();

        // create a new test case using the new configuration and either a new playbook or an existing one
        int testcaseId = creator.createTestcase(testcaseConfig, playbook);

        // apparently everything went smooth. Commit transaction
        creator.saveTestcaseToDB();

        // all done, dispose of the terminal
        creator.end();
    }

}
