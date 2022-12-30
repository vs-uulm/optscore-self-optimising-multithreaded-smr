package de.optscore.vscale;

/**
 * <ul>
    *     <li>
    *         <b>request profile 0</b>
    *         - NOOP (do nothing and return immediately once the request has been ordered)
    *     </li>
    *     <li>
    *         <b>request profile 1</b>
    *         - C250 (simulate load 250µs (0,25ms))
    *     </li>
    *     <li>
    *         <b>request profile 2</b>
    *         - C1000 (simulate load 1000µs (1ms))
    *     </li>
    *     <li>
    *         <b>request profile 3</b>
    *         - L_32 250 U (lock 1 out of 32 Locks randomly, simulate load 250µs, unlock the Lock)
    *     </li>
    *     <li>
    *         <b>request profile 4</b>
    *         - L_1 250 U (lock 1 out of 1 Locks, simluate load 250µs, unlock the Lock)
    *     </li>
    *     <li>
    *         <b>request profile 5</b>
    *         - LU 250 LU (lock and unlock 1 out of 1 Locks, simluate load 250µs, lock and unlock the Lock again)
    *     </li>
 * </ul>
 */
public enum RequestProfile {
    NOOP(0),
    C250(1),
    C1000(2),
    L32_250_U(3),
    L_250_U(4),
    LU_250_LU(5),
    LU_1000_LU(6),
    LU_4000_LU(7),
    LU_250_X3(8),
    L_250_U_LU(9),
    C250_L_50_U_LU(10),
    LU_500_LU(11);

    private final int profileId;

    RequestProfile(int profileId) {
        this.profileId = profileId;
    }

    public int getProfileId() {
        return profileId;
    }
}
