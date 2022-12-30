package de.optscore.vscale.coordination;

import de.optscore.vscale.RequestProfile;
import de.optscore.vscale.util.cli.DBClientGroup;
import de.optscore.vscale.util.cli.DBClientMachine;

import java.sql.*;
import java.util.*;

/**
 * Provides methods for manipulating and querying the DMark test case DB
 */
public class DMarkAdapter {

    private String databasePath;

    private Connection conn;

    public DMarkAdapter(String dbPath) {
        this.databasePath = dbPath;
        this.conn = new SQLiteJDBCDriverConnection().connect(databasePath);
    }


    /**
     * Queries the testcase DB for a testcase's data, including its playbook
     * @param testcaseId The Id of the testcase we want all the details and the playbook of. If this is null, the
     *                   playbook/details of all testcases will be returned
     * @return A {@link ResultSet} containing testcase configuration and playbook
     */
    private ResultSet queryTestcase(String testcaseId) throws SQLException {
        if(testcaseId == null || testcaseId.equals("")) {
            throw new SQLException("testcaseId is Parameter of query and can not be null/empty");
        }

        String testcaseQuery = "SELECT\n" +
                "  t.testcaseId,\n" +
                "  t.description testcaseDesc,\n" +
                "  t.runsCompleted,\n" +
                "  c.gitCommit,\n" +
                "  c.UDSOnOff,\n" +
                "  c.numOfActiveCores,\n" +
                "  c.UDSPrims,\n" +
                "  c.UDSSteps,\n" +
                "  c.replicas,\n" +
                "  wp.timeOffsetNs,\n" +
                "  wp.addRemoveModifier,\n" +
                "  cm.clientMachineId,\n" +
                "  cm.IP,\n" +
                "  cm.port,\n" +
                "  cg.clientGroupId,\n" +
                "  cg.numOfClients,\n" +
                "  cg.requestProfile,\n" +
                "  cg.sendDelayNs\n" +
                "FROM testcase t\n" +
                "  JOIN configuration c ON t.configurationId = c.configurationId\n" +
                "  JOIN workload w ON c.workloadId = w.workloadId\n" +
                "  JOIN workloadPlaybook wp ON w.workloadId = wp.workloadId\n" +
                "  JOIN clientMachine cm ON wp.clientMachineId = cm.clientMachineId\n" +
                "  JOIN clientGroup cg ON wp.clientGroupId = cg.clientGroupId\n" +
                "WHERE t.testcaseId = ?\n" +
                "ORDER BY t.testcaseId, wp.timeOffsetNs\n" +
                "  ASC";

        try {
            PreparedStatement pstmt = conn.prepareStatement(testcaseQuery);
            pstmt.setString(1, testcaseId);
            return pstmt.executeQuery();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<DBClientMachine> queryClientMachines() {
        List<DBClientMachine> clientMachineList = new LinkedList<>();

        String clientMachineQuery = "SELECT * FROM clientMachine";
        try {
            PreparedStatement pstmt = conn.prepareStatement(clientMachineQuery);
            ResultSet rs = pstmt.executeQuery();

            while(rs.next()) {
                int column = 0;
                clientMachineList.add(new DBClientMachine(rs.getInt(++column),
                        rs.getString(++column),
                        rs.getInt(++column),
                        rs.getString(++column)));
            }

            return clientMachineList;
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int insertNewClientMachine(DBClientMachine newClientMachine) {
        String createClientMachineQuery = "INSERT\n" +
                "INTO clientMachine (IP, port, description) \n" +
                "VALUES (?, ?, ?)";
        try {
            PreparedStatement pstmt = conn.prepareStatement(createClientMachineQuery);
            pstmt.setString(1, newClientMachine.getIP());
            pstmt.setInt(2, newClientMachine.getPort());
            pstmt.setString(3, newClientMachine.getDescription());
            pstmt.executeUpdate();
            return pstmt.getGeneratedKeys().getInt(1);
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public List<String> queryWorkloads() {
        List<String> workloadList = new LinkedList<>();
        String workloadQuery = "SELECT * FROM workload";
        try {
            PreparedStatement pstmt = conn.prepareStatement(workloadQuery);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                int column = 0;
                workloadList.add(rs.getInt(++column) + " | " + rs.getString(++column));
            }
            return workloadList;
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int insertNewWorkload(String description) {
        String createWorkloadQuery = "INSERT\n" +
                "INTO workload (description) \n" +
                "VALUES (?)";
        try {
            PreparedStatement pstmt = conn.prepareStatement(createWorkloadQuery);
            pstmt.setString(1, description);
            pstmt.executeUpdate();
            return pstmt.getGeneratedKeys().getInt(1);
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public List<DBClientGroup> queryClientGroups() {
        List<DBClientGroup> clientGroupList = new LinkedList<>();
        String clientGroupQuery = "SELECT * FROM clientGroup";
        try {
            PreparedStatement pstmt = conn.prepareStatement(clientGroupQuery);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                int column = 0;
                clientGroupList.add(new DBClientGroup(rs.getInt(++column),
                        rs.getInt(++column),
                        RequestProfile.values()[rs.getInt(++column)],
                        rs.getLong(++column),
                        rs.getString(++column)));
            }
            return clientGroupList;
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int insertNewClientGroup(DBClientGroup newClientGroup) {
        String createClientGroupQuery = "INSERT\n" +
                "INTO clientGroup (numOfClients, requestProfile, sendDelayNs, description) \n" +
                "VALUES (?, ?, ?, ?)";
        try {
            PreparedStatement pstmt = conn.prepareStatement(createClientGroupQuery);
            pstmt.setInt(1, newClientGroup.getNumOfClients());
            pstmt.setInt(2, newClientGroup.getRequestProfile().getProfileId());
            pstmt.setLong(3, newClientGroup.getSendDelayNs());
            pstmt.setString(4, newClientGroup.getDescription());
            pstmt.executeUpdate();
            return pstmt.getGeneratedKeys().getInt(1);
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public WorkloadPlaybook queryPlaybookActionsForWorkload(int workloadId) {
        WorkloadPlaybook playbook = new WorkloadPlaybook("not yet determined");
        String playbookQuery = "SELECT\n" +
                "       wp.timeOffsetNs,\n" +
                "       wp.addRemoveModifier,\n" +
                "       wp.clientMachineId,\n" +
                "       m.IP,\n" +
                "       m.port,\n" +
                "       m.description,\n" +
                "       wp.clientGroupId,\n" +
                "       g.numOfClients,\n" +
                "       g.requestProfile,\n" +
                "       g.sendDelayNs,\n" +
                "       g.description\n" +
                "FROM workload w\n" +
                "       JOIN workloadPlaybook wp on w.workloadId = wp.workloadId\n" +
                "       JOIN clientGroup g on wp.clientGroupId = g.clientGroupId\n" +
                "       JOIN clientMachine m on wp.clientMachineId = m.clientMachineId\n" +
                "WHERE w.workloadId = ?";
        try {
            PreparedStatement pstmt = conn.prepareStatement(playbookQuery);
            pstmt.setInt(1, workloadId);
            ResultSet rs = pstmt.executeQuery();
            PlaybookAction action;
            Map<Integer, DBClientMachine> clientMachines = new HashMap<>(4);
            Map<Integer, DBClientGroup> clientGroups = new HashMap<>(4);
            int column;
            while(rs.next()) {
                column = 0;
                action = new PlaybookAction();
                action.setTimeOffsetNs(rs.getLong(++column));
                action.setAddRemoveModifier(rs.getInt(++column));

                int machineId = rs.getInt(++column);
                if(clientMachines.containsKey(machineId)) {
                    action.setClientMachine(clientMachines.get(machineId));
                    // skip the client machine columns
                    column += 3;
                } else {
                    clientMachines.put(machineId, new DBClientMachine(machineId,
                            rs.getString(++column),
                            rs.getInt(++column),
                            rs.getString(++column)));
                    action.setClientMachine(clientMachines.get(machineId));
                }

                int groupId = rs.getInt(++column);
                if(clientGroups.containsKey(groupId)) {
                    action.setClientGroup(clientGroups.get(groupId));
                } else {
                    clientGroups.put(groupId, new DBClientGroup(groupId,
                            rs.getInt(++column),
                            RequestProfile.values()[rs.getInt(++column)],
                            rs.getLong(++column),
                            rs.getString(++column)));
                    action.setClientGroup(clientGroups.get(groupId));
                }
                playbook.addPlaybookAction(action);
            }
            return playbook;
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void insertNewPlaybookAction(PlaybookAction action, int workloadId) {
        String createWorkloadPlaybookEntryQuery = "INSERT\n" +
                "OR IGNORE INTO workloadPlaybook (timeOffsetNs, clientMachineId, workloadId, clientGroupId, addRemoveModifier) \n" +
                "VALUES (?, ?, ?, ?, ?)";
        try {
            PreparedStatement pstmt = conn.prepareStatement(createWorkloadPlaybookEntryQuery);
            pstmt.setLong(1, action.getTimeOffsetNs());
            pstmt.setInt(2, action.getClientMachine().getId());
            pstmt.setInt(3, workloadId);
            pstmt.setInt(4, action.getClientGroup().getId());
            pstmt.setInt(5, action.getAddRemoveModifier());
            pstmt.executeUpdate();
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a new test case config in the DB
     * @param conf the configuration to insert into the DB
     * @return the generated Id of the config
     */
    public int insertNewTestcaseConfiguration(TestcaseConfiguration conf) {
        String createTestcaseConfQuery = "INSERT\n" +
                "INTO configuration (gitCommit, workloadId, UDSOnOff, numOfActiveCores, UDSPrims, UDSSteps, replicas) \n" +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try {
            PreparedStatement pstmt = conn.prepareStatement(createTestcaseConfQuery);
            pstmt.setString(1, conf.testcaseGitCommit);
            pstmt.setInt(2, conf.workloadId);
            pstmt.setBoolean(3, conf.withUDS);
            pstmt.setInt(4, conf.numOfActiveCores);
            pstmt.setInt(5, conf.udsConfPrim);
            pstmt.setInt(6, conf.udsConfSteps);
            pstmt.setString(7, conf.getReplicasAsString());
            pstmt.execute();
            return pstmt.getGeneratedKeys().getInt(1);
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int insertNewTestcase(Testcase testcase) {
        String createTestcaseQuery = "INSERT\n" +
                "INTO testcase (testcaseId, description, creationTimestamp, configurationId) \n" +
                "VALUES (?, ?, ?, ?)";
        try {
            PreparedStatement pstmt = conn.prepareStatement(createTestcaseQuery);
            pstmt.setString(1, testcase.getId());
            pstmt.setString(2, testcase.getDescription());
            pstmt.setString(3, new Timestamp(System.currentTimeMillis()).toString());
            pstmt.setInt(4, testcase.getTestcaseConfiguration().id);
            pstmt.executeUpdate();
            return pstmt.getGeneratedKeys().getInt(1);
        } catch(SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public void incrementTestcaseRunNumber(String testcaseId) {
        String incrementRunQuery = "UPDATE testcase\n" +
                "SET runsCompleted = runsCompleted + 1\n" +
                "WHERE testcaseId = ?";
        try {
            PreparedStatement pstmt = conn.prepareStatement(incrementRunQuery);
            pstmt.setString(1, testcaseId);
            pstmt.executeUpdate();
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    public Set<String> queryReplicaConfigurations() {
        Set<String> replicaConfigList = new HashSet<>(3);
        String clientGroupQuery = "SELECT replicas FROM configuration";
        try {
            PreparedStatement pstmt = conn.prepareStatement(clientGroupQuery);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()) {
                int column = 0;
                replicaConfigList.add(rs.getString(++column));
            }
            return replicaConfigList;
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Extracts {@link PlaybookAction}s from a testcase query {@link ResultSet} and returns a populated {@link WorkloadPlaybook}
     * which can be used to start a testcase
     */
    public Testcase objectifyTestcase(String testcaseId) {
        TestcaseConfiguration testcaseConfiguration = new TestcaseConfiguration(testcaseId);
        WorkloadPlaybook playbook = new WorkloadPlaybook(testcaseId);

        String testcaseDescription = "";
        String testcaseGitCommit = "";
        int runsCompleted = 0;
        boolean configUDSOnOff = false;
        int configNumOfActiveCores = 0;
        int configUDSPrims = 0;
        int configUDSSteps = 0;

        try {
            ResultSet rs = queryTestcase(testcaseId);
            while(rs.next()) {

                // extract stuff from the ResultSet
                int column = 1;
                testcaseDescription = rs.getString(++column);
                runsCompleted = rs.getInt(++column);
                testcaseGitCommit = rs.getString(++column);
                configUDSOnOff = rs.getBoolean(++column);
                configNumOfActiveCores = rs.getInt(++column);
                configUDSPrims = rs.getInt(++column);
                configUDSSteps = rs.getInt(++column);
                testcaseConfiguration.setReplicas(rs.getString(++column));

                PlaybookAction action = new PlaybookAction();
                action.setTimeOffsetNs(rs.getLong(++column));
                action.setAddRemoveModifier(rs.getInt(++column));
                DBClientMachine machine = new DBClientMachine();
                machine.setId(rs.getInt(++column));
                machine.setIP(rs.getString(++column));
                machine.setPort(rs.getInt(++column));
                DBClientGroup group = new DBClientGroup();
                group.setId(rs.getInt(++column));
                group.setNumOfClients(rs.getInt(++column));
                group.setRequestProfile(RequestProfile.values()[rs.getInt(++column)]);
                group.setSendDelayNs(rs.getLong(++column));
                action.setClientMachine(machine);
                action.setClientGroup(group);

                playbook.addPlaybookAction(action);
            }

            testcaseConfiguration.testcaseDesc = testcaseDescription;
            testcaseConfiguration.runsCompleted = runsCompleted;
            testcaseConfiguration.testcaseGitCommit = testcaseGitCommit;
            testcaseConfiguration.withUDS = configUDSOnOff;
            testcaseConfiguration.numOfActiveCores = configNumOfActiveCores;
            testcaseConfiguration.udsConfPrim = configUDSPrims;
            testcaseConfiguration.udsConfSteps = configUDSSteps;

        } catch(SQLException e) {
            e.printStackTrace();
            System.err.println("Could not objectify testcase " + testcaseId + ". Aborting ...");
            System.exit(10);
        }

        return new Testcase(testcaseConfiguration, playbook, testcaseConfiguration.testcaseDesc);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    public void commitTransaction() throws SQLException {
        conn.commit();
    }

    public void rollbackTransaction() throws SQLException {
        conn.rollback();
    }

    public void closeConnection() {
        try {
            this.conn.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    private class SQLiteJDBCDriverConnection {

        public Connection connect(String databasePath) {
            Connection conn;
            String connUrl = "jdbc:sqlite:" + databasePath;

            try {
                conn = DriverManager.getConnection(connUrl);
                return conn;
            } catch(SQLException e) {
                e.printStackTrace();
            }
            return null;
        }

    }

}
