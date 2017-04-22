package io.vntr.hermar;

import io.vntr.RepUser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.hermar.BEFRIEND_REBALANCE_STRATEGY.TWO_TO_ONE;
import static io.vntr.hermar.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.hermar.BEFRIEND_REBALANCE_STRATEGY.ONE_TO_TWO;

/**
 * Created by robertlindquist on 4/10/17.
 */
public class HermarBefriendingStrategy {

    private HermarManager manager;

    public HermarBefriendingStrategy(HermarManager manager) {
        this.manager = manager;
    }

    public BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(RepUser user1, RepUser user2) {

        int stay = calcEdgeCutStay(user1, user2);
        int to2  = calcEdgeCutMove(user1, user2);
        int to1  = calcEdgeCutMove(user2, user1);

        double usersPerPartition = ((double) manager.getNumUsers()) / manager.getAllPartitionIds().size();
        int usersOn1 = manager.getPartitionById(user1.getBasePid()).getNumUsers();
        int usersOn2 = manager.getPartitionById(user2.getBasePid()).getNumUsers();

        boolean oneWouldBeOverweight = ((1D + usersOn1) / usersPerPartition) > manager.getGamma();
        boolean twoWouldBeOverweight = ((1D + usersOn2) / usersPerPartition) > manager.getGamma();

        return determineStrategy(stay, to1, to2, usersOn1, usersOn2, oneWouldBeOverweight, twoWouldBeOverweight);
    }

    static BEFRIEND_REBALANCE_STRATEGY determineStrategy(int stay, int to1, int to2, int usersOn1, int usersOn2, boolean oneWouldBeOverweight, boolean twoWouldBeOverweight) {
        if (stay <= to1 && stay <= to2) {
            return NO_CHANGE;
        }

        if (to1 <= stay && to1 <= to2 && !oneWouldBeOverweight) {
            if (usersOn1 < usersOn2) {
                return TWO_TO_ONE;
            }

            float imbalanceRatio = (usersOn1 + 1f) / usersOn2;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, to2)) / to1;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return TWO_TO_ONE;
            }
        }

        if (to2 <= stay && to2 <= to1 && !twoWouldBeOverweight) {
            if (usersOn2 < usersOn1) {
                return ONE_TO_TWO;
            }

            float imbalanceRatio = (usersOn2 + 1f) / usersOn1;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, to1)) / to2;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return ONE_TO_TWO;
            }
        }

        return NO_CHANGE;
    }

    int calcEdgeCutStay(RepUser user1, RepUser user2) {
        return getNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions(user1.getBasePid(), user2.getBasePid());
    }

    int calcEdgeCutMove(RepUser movingUser, RepUser stayingUser) {
        int movingPid = movingUser.getBasePid();
        int stayingPid = stayingUser.getBasePid();
        int currentEdgeCut = getNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions(movingPid, stayingPid);

        Map<Integer, Integer> pToFriendCount = getPToFriendCount(movingUser.getId());
        int friendsOnMovingPartition = pToFriendCount.get(movingPid);
        int friendsOnStayingPartition = pToFriendCount.get(stayingPid);

        return currentEdgeCut + friendsOnMovingPartition - friendsOnStayingPartition;
    }

    Map<Integer, Integer> getPToFriendCount(Integer uid) {
        Map<Integer, Integer> pToFriendCount = new HashMap<>();
        for(Integer pid : manager.getAllPartitionIds()) {
            pToFriendCount.put(pid, 0);
        }
        for(Integer friendId : manager.getUser(uid).getFriendIDs()) {
            Integer pid = manager.getPartitionIdForUser(friendId);
            pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
        }
        return pToFriendCount;
    }

    Integer getNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions(int pid1, int pid2) {
        int count = 0;
        Set<Integer> idsThatMatter = new HashSet<>(manager.getPartitionById(pid1).getPhysicalUserIds());
        idsThatMatter.addAll(manager.getPartitionById(pid2).getPhysicalUserIds());

        for(Integer uid : idsThatMatter) {
            Map<Integer, Integer> pToFriendCount = getPToFriendCount(uid);
            for(Integer pid : manager.getAllPartitionIds()) {
                if(!pid.equals(manager.getPartitionIdForUser(uid)) && pToFriendCount.containsKey(pid)) {
                    count += pToFriendCount.get(pid);
                }

            }
        }
        return count / 2;
    }

}
