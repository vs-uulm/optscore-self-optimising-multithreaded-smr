package de.optscore.vscale;

import java.util.Random;

/**
 * Provides access to preconfigured request profiles that can be used in test cases.
 * The test case DB references these by an index that is not explained in the DB itself. This is suboptimal, but the
 * way it currently is ...
 */
public class RequestProfileRepository {

    private EvalRequest[] repository;
    private EvalRequest[] workload3Reqs;
    private Random random;

    /**
     * Create a new standard Repository
     *
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
     *         - LU_250_LU (lock and unlock 1 out of 1 Locks, simluate load 250µs, lock and unlock the Lock)
     *     </li>
     * </ul>
     */
    public RequestProfileRepository() {
        this.random = new Random();
        this.initRepository();
    }

    private void initRepository() {
        // create a new repo, size depends on the number of different profiles we use while testing our entire platform
        this.repository = new EvalRequest[RequestProfile.values().length];

        // for requestProfile 3 we need random lock stuff, so we prepare 32 requests, one for each lock, and pick one
        this.workload3Reqs = new EvalRequest[32];
        for(int i = 0; i < workload3Reqs.length; i++) {
            workload3Reqs[i] = new EvalRequest.EvalRequestBuilder()
                    .action(EvalActionType.LOCK.getActionTypeCode(), i)
                    .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 250000)
                    .action(EvalActionType.UNLOCK.getActionTypeCode(), i)
                    .build();
        }

        // populate the repository
        repository[RequestProfile.NOOP.getProfileId()] =
                new EvalRequest.EvalRequestBuilder().action(EvalActionType.READONLY.getActionTypeCode(), 0).build();
        repository[RequestProfile.C250.getProfileId()] =
                new EvalRequest.EvalRequestBuilder().action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 250000).build();
        repository[RequestProfile.C1000.getProfileId()] =
                new EvalRequest.EvalRequestBuilder().action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 1000000).build();
        repository[RequestProfile.L32_250_U.getProfileId()] = null; // request profile 3 is randomized, see getRequestByProfileId() method
        repository[RequestProfile.L_250_U.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 250000)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .build();
        repository[RequestProfile.LU_250_LU.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 250000)
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .build();
        repository[RequestProfile.LU_1000_LU.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK.getActionTypeCode(), 1)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 1)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 1000000)
                .action(EvalActionType.LOCK.getActionTypeCode(), 1)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 1)
                .build();
        repository[RequestProfile.LU_4000_LU.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 4000000)
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .build();
        repository[RequestProfile.LU_250_X3.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 250000)
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .build();
        repository[RequestProfile.L_250_U_LU.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 250000)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .action(EvalActionType.LOCK.getActionTypeCode(), 1)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 1)
                .build();
        repository[RequestProfile.LU_500_LU.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK.getActionTypeCode(), 1)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 1)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 500000)
                .action(EvalActionType.LOCK.getActionTypeCode(), 1)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 1)
                .build();
        repository[RequestProfile.C250_L_50_U_LU.getProfileId()] = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 250000)
                .action(EvalActionType.LOCK.getActionTypeCode(), 0)
                .action(EvalActionType.SIMULATELOAD.getActionTypeCode(), 50000)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 0)
                .action(EvalActionType.LOCK.getActionTypeCode(), 1)
                .action(EvalActionType.UNLOCK.getActionTypeCode(), 1)
                .build();
    }

    public EvalRequest getRequestForProfile(RequestProfile profile) {
        if(profile.getProfileId() == 3) {
            return workload3Reqs[random.nextInt(workload3Reqs.length)];
        } else {
            return repository[profile.getProfileId()];
        }
    }

}
