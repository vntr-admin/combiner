package io.vntr.befriend;

import io.vntr.repartition.HRepartitioner;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.utils.Utils.getUToMasterMap;

/**
 * Created by robertlindquist on 4/30/17.
 */
public class HBefriender2 {
    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(int smallUid, int largeUid, float gamma, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) {
        Set<Integer> uids = new HashSet<>();
        uids.add(smallUid);
        uids.add(largeUid);
        Map<Integer, HRepartitioner.LogicalUser> lusers = HRepartitioner.initLogicalUsers(partitions, friendships, uids, gamma);
        HRepartitioner.LogicalUser smallUser = lusers.get(smallUid);
        HRepartitioner.LogicalUser largeUser = lusers.get(largeUid);

        int smallToLargeGain = calculateGain(smallUser, largeUser.getPid(), gamma);
        int largeToSmallGain = calculateGain(largeUser, smallUser.getPid(), gamma);

        if(smallToLargeGain > largeToSmallGain && smallToLargeGain > 0) {
            return BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
        }
        else if(largeToSmallGain > smallToLargeGain && largeToSmallGain > 0) {
            return BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
        }

        return BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
    }

    static int calculateGain(HRepartitioner.LogicalUser user, int pid, float gamma) {
        int gain = user.getpToFriendCount().get(pid) - user.getpToFriendCount().get(user.getPid());
        return (user.getImbalanceFactor(pid, 1) < gamma) ? gain : -1;
    }

}
