package io.vntr.befriend;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.repartition.HRepartitioner;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.utils.TroveUtils.convertMapSetToTIntObjectMapTIntSet;

/**
 * Created by robertlindquist on 4/30/17.
 */
public class HBefriender {
    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(int smallUid, int largeUid, float gamma, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) {
        Set<Integer> uids = new HashSet<>();
        uids.add(smallUid);
        uids.add(largeUid);
        TIntObjectMap<HRepartitioner.LogicalUser> lusers = HRepartitioner.initLogicalUsers(convertMapSetToTIntObjectMapTIntSet(partitions), convertMapSetToTIntObjectMapTIntSet(friendships), new TIntHashSet(uids), gamma);
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
