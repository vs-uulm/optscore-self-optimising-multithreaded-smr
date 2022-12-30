package de.optscore.vscale.coordination;

import de.optscore.vscale.RequestProfile;
import de.optscore.vscale.util.cli.DBClientGroup;
import de.optscore.vscale.util.cli.DBClientMachine;

import java.util.HashMap;
import java.util.Map;

/**
 * Models a single action that happens during a testcase (i.e. when following a WorkloadPlaybook), e.g.
 * adding or removing a clientGroup at a certain time
 */
public class PlaybookAction {
    private DBClientMachine clientMachine = new DBClientMachine();
    private DBClientGroup clientGroup = new DBClientGroup();

    private long timeOffsetNs;
    private int addRemoveModifier;


    public long getTimeOffsetNs() {
        return timeOffsetNs;
    }

    public void setTimeOffsetNs(long timeOffsetNs) {
        this.timeOffsetNs = timeOffsetNs;
    }

    public int getAddRemoveModifier() {
        return addRemoveModifier;
    }

    public void setAddRemoveModifier(int addRemoveModifier) {
        this.addRemoveModifier = addRemoveModifier;
    }

    public DBClientMachine getClientMachine() {
        return clientMachine;
    }

    public void setClientMachine(DBClientMachine clientMachine) {
        this.clientMachine = clientMachine;
    }

    public DBClientGroup getClientGroup() {
        return clientGroup;
    }

    public void setClientGroup(DBClientGroup clientGroup) {
        this.clientGroup = clientGroup;
    }

    public static PlaybookAction createPlaybookActionFromString(String playbookFileLine) {
        Map<String, String> kvPairs = new HashMap<>(10);

        for(String kvPair : playbookFileLine.split("\\|")) {
            String[] tmp = kvPair.split("\\$");
            if(!tmp[0].startsWith("playbookAction")) {
                kvPairs.put(tmp[0], tmp[1]);
            }
        }

        PlaybookAction action = new PlaybookAction();
        action.setTimeOffsetNs(Long.parseLong(kvPairs.get("timeOffsetNs")));
        action.setAddRemoveModifier(Integer.parseInt(kvPairs.get("addRemoveModifier")));

        DBClientMachine machine = new DBClientMachine();
        machine.setId(Integer.parseInt(kvPairs.get("clientMachineId")));
        machine.setIP(kvPairs.get("clientMachineIP"));
        machine.setPort(Integer.parseInt(kvPairs.get("clientMachinePort")));
        action.setClientMachine(machine);

        DBClientGroup group = new DBClientGroup();
        group.setId(Integer.parseInt(kvPairs.get("clientGroupId")));
        group.setNumOfClients(Integer.parseInt(kvPairs.get("clientGroupNumOfClients")));
        group.setRequestProfile(RequestProfile.values()[Integer.parseInt(kvPairs.get(
                "clientGroupRequestProfile"))]);
        group.setSendDelayNs(Long.parseLong(kvPairs.get("clientGroupSendDelayNs")));
        action.setClientGroup(group);

        return action;
    }

    @Override
    public String toString() {
        return "playbookAction"
                + "|timeOffsetNs$" + getTimeOffsetNs()
                + "|clientMachineId$" + clientMachine.getId()
                + "|clientMachineIP$" + clientMachine.getIP()
                + "|clientMachinePort$" + clientMachine.getPort()
                + "|addRemoveModifier$" + getAddRemoveModifier()
                + "|clientGroupId$" + clientGroup.getId()
                + "|clientGroupNumOfClients$" + clientGroup.getNumOfClients()
                + "|clientGroupRequestProfile$" + clientGroup.getRequestProfile().getProfileId()
                + "|clientGroupSendDelayNs$" + clientGroup.getSendDelayNs();
    }
}
