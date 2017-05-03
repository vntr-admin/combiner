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
    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(int uid1, int uid2, float gamma, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) {
        Set<Integer> uids = new HashSet<>();
        uids.add(uid1);
        uids.add(uid2);
        Map<Integer, HRepartitioner.LogicalUser> lusers = initLogicalUsers(uids, partitions, friendships, gamma);
        HRepartitioner.LogicalUser user1 = lusers.get(uid1);
        HRepartitioner.LogicalUser user2 = lusers.get(uid2);

        int u1ToP2Gain = calculateGain(user1, user2.getPid(), gamma);
        int u2ToP1Gain = calculateGain(user2, user1.getPid(), gamma);

        if(u1ToP2Gain > u2ToP1Gain && u1ToP2Gain > 0) {
            return BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
        }
        else if(u2ToP1Gain > u1ToP2Gain && u2ToP1Gain > 0) {
            return BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
        }

        return BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
    }

    static int calculateGain(HRepartitioner.LogicalUser user, int pid, float gamma) {
        int gain = user.getpToFriendCount().get(pid) - user.getpToFriendCount().get(user.getPid());
        float balanceFactor = user.getImbalanceFactor(pid, 1);
        return (balanceFactor < gamma) ? gain : -1;
    }

    static Map<Integer, HRepartitioner.LogicalUser> initLogicalUsers(Set<Integer> uidsToInit, Map<Integer, Set<Integer>> logicalPids, Map<Integer, Set<Integer>> friendships, float gamma) {
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(logicalPids);

        Map<Integer, Integer> pToWeight = new HashMap<>();
        int totalWeight = 0;
        for(int pid : logicalPids.keySet()) {
            int numUsersOnPartition = logicalPids.get(pid).size();
            pToWeight.put(pid, numUsersOnPartition);
            totalWeight += numUsersOnPartition;
        }

        Map<Integer, HRepartitioner.LogicalUser> logicalUsers = new HashMap<>();
        for(int uid : uidsToInit) {
            Map<Integer, Integer> pToFriendCount = new HashMap<>();
            for(int pid : logicalPids.keySet()) {
                pToFriendCount.put(pid, 0);
            }
            for(Integer friendId : friendships.get(uid)) {
                int pid = uidToPidMap.get(friendId);
                pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
            }

            int pid = uidToPidMap.get(uid);
            logicalUsers.put(uid, new HRepartitioner.LogicalUser(uid, pid, gamma, pToFriendCount, new HashMap<>(pToWeight), totalWeight));
        }

        return logicalUsers;
    }
}
