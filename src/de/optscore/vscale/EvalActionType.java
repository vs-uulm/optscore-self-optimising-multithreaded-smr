package de.optscore.vscale;

public enum EvalActionType {
    LOCK(0),
    UNLOCK(1),
    ADDTOSHAREDSTATE(2),
    SIMULATELOAD(3),
    READONLY(4),
    REMOVE_CPU_CORES(5),
    ADD_CPU_CORES(6),
    RECONFIG_UDS_PRIMARIES(7),
    RECONFIG_UDS_STEPS(8),
    STATS_START(9),
    STATS_DUMP(10),
    ByTI(11);

    private final int actionTypeCode;

    EvalActionType(int actionTypeCode) {
        this.actionTypeCode = actionTypeCode;
    }

    public int getActionTypeCode() {
        return actionTypeCode;
    }
}
