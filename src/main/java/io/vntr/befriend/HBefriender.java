package io.vntr.befriend;

import io.vntr.User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.Utils.getUToMasterMap;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class HBefriender {

    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(User user1, User user2, float gamma, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) {

        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        int stay = calcEdgeCutStay(user1, user2, friendships, partitions, uidToPidMap);
        int to2  = calcEdgeCutMove(user1, user2, friendships, partitions, uidToPidMap);
        int to1  = calcEdgeCutMove(user2, user1, friendships, partitions, uidToPidMap);

        double usersPerPartition = ((double) friendships.size()) / partitions.size();
        int usersOn1 = partitions.get(user1.getBasePid()).size();
        int usersOn2 = partitions.get(user2.getBasePid()).size();

        boolean oneWouldBeOverweight = ((1D + usersOn1) / usersPerPartition) > gamma;
        boolean twoWouldBeOverweight = ((1D + usersOn2) / usersPerPartition) > gamma;

        return determineStrategy(stay, to1, to2, usersOn1, usersOn2, oneWouldBeOverweight, twoWouldBeOverweight);
    }

    static BEFRIEND_REBALANCE_STRATEGY determineStrategy(int stay, int to1, int to2, int usersOn1, int usersOn2, boolean oneWouldBeOverweight, boolean twoWouldBeOverweight) {
        if (stay <= to1 && stay <= to2) {
            return NO_CHANGE;
        }

        if (to1 <= stay && to1 <= to2 && !oneWouldBeOverweight) {
            if (usersOn1 < usersOn2) {
                return LARGE_TO_SMALL;
            }

            float imbalanceRatio = (usersOn1 + 1f) / usersOn2;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, to2)) / to1;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return LARGE_TO_SMALL;
            }
        }

        if (to2 <= stay && to2 <= to1 && !twoWouldBeOverweight) {
            if (usersOn2 < usersOn1) {
                return SMALL_TO_LARGE;
            }

            float imbalanceRatio = (usersOn2 + 1f) / usersOn1;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, to1)) / to2;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return SMALL_TO_LARGE;
            }
        }

        return NO_CHANGE;
    }

    static int calcEdgeCutStay(User user1, User user2, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Integer> uidToPidMap) {
        return getNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions(user1.getBasePid(), user2.getBasePid(), friendships, partitions, uidToPidMap);
    }

    static int calcEdgeCutMove(User movingUser, User stayingUser, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Integer> uidToPidMap) {
        int movingPid = movingUser.getBasePid();
        int stayingPid = stayingUser.getBasePid();
        int currentEdgeCut = getNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions(movingPid, stayingPid, friendships, partitions, uidToPidMap);

        Map<Integer, Integer> pToFriendCount = getPToFriendCount(movingUser.getId(), friendships, uidToPidMap);
        int friendsOnMovingPartition = pToFriendCount.get(movingPid);
        int friendsOnStayingPartition = pToFriendCount.get(stayingPid);

        return currentEdgeCut + friendsOnMovingPartition - friendsOnStayingPartition;
    }

    static Map<Integer, Integer> getPToFriendCount(Integer uid, Map<Integer, Set<Integer>> friendships, Map<Integer, Integer> uidToPidMap) {
        Map<Integer, Integer> pToFriendCount = new HashMap<>();
        for(Integer pid : uidToPidMap.keySet()) {
            pToFriendCount.put(pid, 0);
        }
        for(Integer friendId : friendships.get(uid)) {
            Integer pid = uidToPidMap.get(friendId);
            pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
        }
        return pToFriendCount;
    }

    static Integer getNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions(int pid1, int pid2, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Integer> uidToPidMap) {
        int count = 0;
        Set<Integer> idsThatMatter = new HashSet<>(partitions.get(pid1));
        idsThatMatter.addAll(partitions.get(pid2));

        for(Integer uid : idsThatMatter) {
            Map<Integer, Integer> pToFriendCount = getPToFriendCount(uid, friendships, uidToPidMap);
            for(Integer pid : uidToPidMap.keySet()) {
                if(!pid.equals(uidToPidMap.get(uid)) && pToFriendCount.containsKey(pid)) {
                    count += pToFriendCount.get(pid);
                }

            }
        }
        return count / 2;
    }

}
