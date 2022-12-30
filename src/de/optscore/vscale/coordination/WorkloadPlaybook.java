package de.optscore.vscale.coordination;

import de.optscore.vscale.RequestProfile;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A workload playbook specifies exactly when, on which machine which clientGroups are to start/stop sending
 * requests.
 * It deconstructs the saved playbook in the DB into a per-machine view, and performs sanity checks (e.g. all added [
 * = started sending] clientGroups need to be removed [= stopped sending] at the end of testcase, etc).
 * A playbook can then be used to save playbookActions to the client machine files, after which a testcase can be
 * started.
 */
public class WorkloadPlaybook {

    /**
     * The testcaseId of the test case this playbook is for
     */
    private final String testcaseId;

    private List<PlaybookAction> playbookActions;

    private boolean valid;
    private long playbookDuration = -1L;
    int maxSimultaneouslyActiveClientsOverall = -1;

    // smallest client pid
    private final int minPid = 128;

    public WorkloadPlaybook(String testcaseId) {
        this.testcaseId = testcaseId;
        this.playbookActions = new ArrayList<>(20);
        this.valid = false;
    }

    /**
     * Adds an action to this playbook (e.g. add/remove clientgroup)
     * @param action
     */
    public void addPlaybookAction(PlaybookAction action) {
        playbookActions.add(action);
    }

    public boolean isValid() {
        // re-validate playbook
        try {
            validatePlaybook();
        } catch(InvalidWorkloadPlaybookException e) {
            // do not print the exception
            valid = false;
            return false;
        }
        valid = true;
        return true;
    }

    private void validatePlaybook() throws InvalidWorkloadPlaybookException {
        Set<Integer> clientMachineIds =
                playbookActions.stream().map(action -> action.getClientMachine().getId())
                .collect(Collectors.toSet());

        boolean valid = true;
        for(int i : clientMachineIds) {
            valid &= validateClientMachinePlaybook(playbookActions.stream()
                    .filter(playbookAction -> playbookAction.getClientMachine().getId() == i)
                    .collect(Collectors.toList()));
        }
        this.valid = valid;
        if(!valid) {
            throw new InvalidWorkloadPlaybookException(this.testcaseId);
        }
    }

    /**
     * Get the next action (by its timeOffsetNs) after the passed currentTimeOffsetNs
     * @param currentTimeOffsetNs the time after which the next action should be searched
     * @return The {@link PlaybookAction} with the smallest timeOffsetNs > currentTimeOffset
     */
    public Optional<PlaybookAction> getNextAction(long currentTimeOffsetNs) {
        return playbookActions.stream()
                .filter(playbookAction -> playbookAction.getTimeOffsetNs() > currentTimeOffsetNs)
                .findFirst();
    }

    /**
     * Get all actions that have to be executed (i.e. are still in the playbook but their timeOffsetNs is < currentTime
     * @param currentTimeOffsetNs The timeOffset of the current moment, i.e. where the test is currently at
     * @return An array of {@link PlaybookAction}s with all actions that have to be executed
     */
    public PlaybookAction[] getExecutableActions(long currentTimeOffsetNs) {
        return playbookActions.stream()
                .filter(playbookAction ->
                        playbookAction.getTimeOffsetNs() <= currentTimeOffsetNs && playbookAction.getTimeOffsetNs() >= 0L)
                .toArray(PlaybookAction[]::new);
    }

    public void removeActionFromPlaybook(PlaybookAction action) {
        playbookActions.remove(action);
    }

    public Integer[] getClientMachineIds() {
        return playbookActions.stream().map(action -> action.getClientMachine().getId()).distinct().toArray(Integer[]::new);
    }

    public String getClientMachineIp(int machineId) {
        return playbookActions.stream()
                .filter(action -> action.getClientMachine().getId() == machineId)
                .findAny()
                .get()
                .getClientMachine().getIP();
    }

    private int getMinPidForClientMachine(int machineId) {
        Set<Integer> clientMachineIds = playbookActions.stream().map(action -> action.getClientMachine().getId())
                .collect(Collectors.toSet());

        int clientCountAccumulator = 0;
        for(int i : clientMachineIds) {
            if(i < machineId) {
                clientCountAccumulator += getActiveClientsCountForMachine(i);
            }
        }

        return minPid + clientCountAccumulator;
    }

    private int getMaxPidForClientMachine(int machineId) {
        Set<Integer> clientMachineIds = playbookActions.stream()
                .map(action -> action.getClientMachine().getId())
                .collect(Collectors.toSet());

        int clientCountAccumulator = 0;
        for(int i : clientMachineIds) {
            if(i <= machineId) {
                clientCountAccumulator += getActiveClientsCountForMachine(i);
            }
        }

        return minPid + clientCountAccumulator - 1;
    }

    public Map<Integer, Integer> getActiveClientGroupsCountByIdForMachine(int machineId) {
        List<PlaybookAction> machinePlaybook = playbookActions.stream()
                .filter(playbookAction -> playbookAction.getClientMachine().getId() == machineId)
                .collect(Collectors.toList());

        Map<Integer, Integer> maxActiveClientGroupMap = new HashMap<>();
        for(PlaybookAction action : machinePlaybook) {
            int clientGroupId = action.getClientGroup().getId();
            int addRemoveModifier = action.getAddRemoveModifier();

            // initialize if we haven't seen this clientGroup yet
            maxActiveClientGroupMap.putIfAbsent(clientGroupId, 0);

            // calculate the new max
            int currentTally = maxActiveClientGroupMap.get(clientGroupId);
            int newTally = currentTally + addRemoveModifier;

            // only update the map if the new max is bigger than the previously saved max
            if(newTally > currentTally) {
                maxActiveClientGroupMap.merge(clientGroupId, addRemoveModifier, (prevVal, newVal) -> prevVal + newVal);
            }
        }
        return maxActiveClientGroupMap;
    }

    /**
     * Returns the max. number of simultaneously active clients during the test
     * @return highest number of concurrently active clients during this test
     */
    public int getMaxSimultaneouslyActiveClientsOverall() {
        if(maxSimultaneouslyActiveClientsOverall == -1) {
            int clientCountAccumulator = 0;
            maxSimultaneouslyActiveClientsOverall = 0;
            for(PlaybookAction action : playbookActions) {
                clientCountAccumulator += action.getAddRemoveModifier() * action.getClientGroup().getNumOfClients();
                if(clientCountAccumulator > maxSimultaneouslyActiveClientsOverall) {
                    maxSimultaneouslyActiveClientsOverall = clientCountAccumulator;
                }
            }
        }
        return maxSimultaneouslyActiveClientsOverall;
    }

    /**
     * Returns the max. number of active clients on a single machine during the test
     * @param machineId identifies the machine
     * @return total number of active clients on machine with machineId during this test
     */
    public int getActiveClientsCountForMachine(int machineId) {
        List<PlaybookAction> machinePlaybook = playbookActions.stream()
                .filter(playbookAction ->
                        playbookAction.getClientMachine().getId() == machineId
                        && playbookAction.getAddRemoveModifier() > 0
                )
                .collect(Collectors.toList());

        int clientCountAccumulator = 0;
        for(PlaybookAction action : machinePlaybook) {
            clientCountAccumulator += action.getAddRemoveModifier() * action.getClientGroup().getNumOfClients();
        }
        return clientCountAccumulator;
    }

    public List<PlaybookAction> getPlaybookActions() {
        return playbookActions;
    }

    public long getPlaybookDuration() {
        if(playbookDuration == -1L) {
            playbookDuration = playbookActions.stream().
                    max(Comparator.comparingLong(PlaybookAction::getTimeOffsetNs)).get().getTimeOffsetNs();
        }
        return playbookDuration;
    }

    public int getClientGroupNumOfClients(int clientGroupId) {
        return playbookActions.stream()
                .filter(playbookAction -> playbookAction.getClientGroup().getId() == clientGroupId)
                .findAny()
                .get()
                .getClientGroup().getNumOfClients();
    }

    public RequestProfile getClientGroupRequestProfile(int clientGroupId) {
        return playbookActions.stream()
                .filter(playbookAction -> playbookAction.getClientGroup().getId() == clientGroupId)
                .findAny()
                .get()
                .getClientGroup().getRequestProfile();
    }

    public long getClientGroupSendDelay(int clientGroupId) {
        return playbookActions.stream()
                .filter(playbookAction -> playbookAction.getClientGroup().getId() == clientGroupId)
                .findAny()
                .get()
                .getClientGroup().getSendDelayNs();
    }

    /**
     * Serializes a machine's playbook so it can be passed to another machine via file/network/etc
     *
     * @param machineId The machine that should get the playbook
     * @return A String containing a serialized playbook for this machine
     */
    public String serializePlaybookForMachine(int machineId, int runNumber, int numOfCoresToActivate, int udsPrims,
                                              int udsSteps) {
        return playbookActions.stream()
                .filter(playbookAction -> playbookAction.getClientMachine().getId() == machineId)
                .map(PlaybookAction::toString)
                .collect(Collectors.joining("\n", "testcaseId$" + this.testcaseId
                        + "|machineId$" + machineId
                        + "|minPid$" + this.getMinPidForClientMachine(machineId)
                        + "|maxPid$" + this.getMaxPidForClientMachine(machineId)
                        + "|runNumber$" + runNumber
                        + "|numOfCoresToActivate$" + numOfCoresToActivate
                        + "|udsPrims$" + udsPrims
                        + "|udsSteps$" + udsSteps
                        + "\n", "\n"));
    }

    /**
     * Serializes the entire WorkloadPlaybook so it can be logged or saved to disk for documentation purposes
     * @return A serialized version of this entire playbook (i.e. all its actions)
     */
    public String serializePlaybook() {
        return playbookActions.stream()
                .map(PlaybookAction::toString)
                .collect(Collectors.joining("\n", "WorkloadPlaybook:\ntestcaseId$" + this.testcaseId
                        + "\n", "\n"));
    }

    private boolean validateClientMachinePlaybook(List<PlaybookAction> machinePlaybook) {
        Set<Integer> clientGroupIds = machinePlaybook.stream()
                .map(action -> action.getClientGroup().getId())
                .collect(Collectors.toSet());

        boolean valid = true;
        for(int i : clientGroupIds) {
            valid &= validateClientMachinePlaybookForClientGroup(machinePlaybook.stream()
                    .filter(playbookAction ->
                    playbookAction.getClientGroup().getId() == i)
                    .collect(Collectors.toList()));
        }

        return valid;
    }

    private boolean validateClientMachinePlaybookForClientGroup(List<PlaybookAction> machineClientGroupPlaybook) {
        int validator = 0;
        for(PlaybookAction action : machineClientGroupPlaybook) {
            validator += action.getAddRemoveModifier();
        }
        //TODO validate whether clientGroups are added in the right order (no group is removed before it has been added)

        return validator == 0;
    }

}
