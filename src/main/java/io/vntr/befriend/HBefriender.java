package io.vntr.befriend;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.repartition.HRepartitioner;

/**
 * Created by robertlindquist on 4/30/17.
 */
public class HBefriender {
    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(short smallUid, short largeUid, float gamma, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions) {
        TShortSet uids = new TShortHashSet();
        uids.add(smallUid);
        uids.add(largeUid);
        TShortObjectMap<HRepartitioner.LogicalUser> lusers = HRepartitioner.initLogicalUsers(partitions, friendships, new TShortHashSet(uids), gamma);
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

    static int calculateGain(HRepartitioner.LogicalUser user, short pid, float gamma) {
        int gain = user.getpToFriendCount().get(pid) - user.getpToFriendCount().get(user.getPid());
        return (user.getImbalanceFactor(pid, (short) 1) < gamma) ? gain : -1;
    }

}
