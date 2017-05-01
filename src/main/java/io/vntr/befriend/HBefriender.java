package io.vntr.befriend;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.utils.Utils.getPToFriendCount;
import static io.vntr.utils.Utils.getUToMasterMap;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class HBefriender {

    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(int uid1, int uid2, float gamma, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) {

        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        int stay = calcEdgeCutStay(uid1, uid2, friendships, partitions, uidToPidMap);
        int to2  = calcEdgeCutMove(uid1, uid2, friendships, partitions, uidToPidMap);
        int to1  = calcEdgeCutMove(uid2, uid1, friendships, partitions, uidToPidMap);

        double usersPerPartition = ((double) friendships.size()) / partitions.size();
        int usersOn1 = partitions.get(uidToPidMap.get(uid1)).size();
        int usersOn2 = partitions.get(uidToPidMap.get(uid2)).size();

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

    static int calcEdgeCutStay(int uid1, int uid2, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Integer> uidToPidMap) {
        return getCutBetweenPartitions(uidToPidMap.get(uid1), uidToPidMap.get(uid2), friendships, partitions, uidToPidMap);
    }

    static int calcEdgeCutMove(int movingUserId, int stayingUserId, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Integer> uidToPidMap) {
        int movingPid = uidToPidMap.get(movingUserId);
        int stayingPid = uidToPidMap.get(stayingUserId);
        int currentEdgeCut = getCutBetweenPartitions(movingPid, stayingPid, friendships, partitions, uidToPidMap);


        Map<Integer, Integer> pToFriendCount = getPToFriendCount(movingUserId, friendships, uidToPidMap, partitions.keySet());
        int friendsOnMovingPartition = pToFriendCount.get(movingPid);
        int friendsOnStayingPartition = pToFriendCount.get(stayingPid);

        return currentEdgeCut + friendsOnMovingPartition - friendsOnStayingPartition;
    }

    static int getCutBetweenPartitions(int pid1, int pid2, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Integer> uidToPidMap) {
        int count = 0;
        for(int uid : partitions.get(pid1)) {
            count += Sets.intersection(friendships.get(uid), partitions.get(pid2)).size();
        }
        return count;
    }
}
