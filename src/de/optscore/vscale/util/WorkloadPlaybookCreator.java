package de.optscore.vscale.util;

import de.optscore.vscale.coordination.PlaybookAction;

import java.util.HashMap;
import java.util.Map;

public class WorkloadPlaybookCreator {

    private static final String table = "workloadPlaybook";

    public static void main(String[] args) {

        long currentOffsetNs = 0L;
        long deltaOffsetNs = 5000000000L;
        int numOfTotalClients = 400;
        int numOfClientMachines = 4;
        int clientMachineIdOffset = 10;
        int clientAddStep = 10;
        int workloadId = 5;

        if(numOfTotalClients % clientAddStep != 0) {
            System.out.println("Can't divide numOfTotalClients by clientAddStep, aborting ...");
            System.exit(1);
        }

        // add and remove clients
        Map<Integer, Integer> clientGroupCountPerMachine = new HashMap<>();
        PlaybookAction action = new PlaybookAction();
        // action.setClientGroupNumOfClients(1);
        // action.setClientGroupId(3);
        action.setAddRemoveModifier(clientAddStep);

        for(int i = 0; i < numOfTotalClients / clientAddStep; i++) {
            action.setTimeOffsetNs(currentOffsetNs);
            // action.setClientMachineId((i % numOfClientMachines) + clientMachineIdOffset);
            // clientGroupCountPerMachine.merge(action.getClientMachineId(), action.getAddRemoveModifier(),
            //        (savedVal, newVal) -> savedVal + newVal);
            /* System.out.println("insert into " + table + " values ("
                    + action.getTimeOffsetNs() + ","
                    + action.getClientMachineId() + ","
                    + workloadId + ","
                    + action.getClientGroupId() + ","
                    + action.getAddRemoveModifier() + ");");
            */
            currentOffsetNs += deltaOffsetNs;
        }

        action.setTimeOffsetNs(currentOffsetNs);
        for(int i : clientGroupCountPerMachine.keySet()) {
            /*System.out.println("insert into " + table + " values ("
                    + action.getTimeOffsetNs() + ","
                    + i + ","
                    + workloadId + ","
                    + action.getClientGroupId() + ","
                    + (Math.negateExact(clientGroupCountPerMachine.get(i))) + ");");
            */
        }

    }
}
