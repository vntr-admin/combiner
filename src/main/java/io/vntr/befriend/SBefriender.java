package io.vntr.befriend;

import io.vntr.RepUser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.utils.Utils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

public class SBefriender {
    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(RepUser smallerUser, RepUser largerUser, int minNumReplicas, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        //calculate the number of replicas that would be generated for each of the three possible configurations:
        //  1) no movements of masters, which maintains the status-quo
        //	2) the master of smallerUserId goes to the partition containing the master of largerUserId
        //  3) the opposite of (3)

        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        int stay      = calcNumReplicasStay(smallerUser, largerUser, replicas);
        int toLarger  = calcNumReplicasMove(smallerUser, largerUser, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, friendships);
        int toSmaller = calcNumReplicasMove(largerUser, smallerUser, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, friendships);

        int smallerMasters = partitions.get(smallerUser.getBasePid()).size();
        int largerMasters  = partitions.get(largerUser.getBasePid()).size();


        return determineStrategy(stay, toSmaller, toLarger, smallerMasters, largerMasters);
    }

    public static BEFRIEND_REBALANCE_STRATEGY determineStrategy(int stay, int toSmaller, int toLarger, int smallerMasters, int largerMasters) {
        if (stay <= toSmaller && stay <= toLarger) {
            return NO_CHANGE;
        }

        if (toSmaller <= stay && toSmaller <= toLarger) {
            if (smallerMasters < largerMasters) {
                return LARGE_TO_SMALL;
            }

            float imbalanceRatio = smallerMasters / largerMasters;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, toLarger)) / toSmaller;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return LARGE_TO_SMALL;
            }
        }

        if (toLarger <= stay && toLarger <= toSmaller) {
            if (largerMasters < smallerMasters) {
                return SMALL_TO_LARGE;
            }

            float imbalanceRatio = largerMasters / smallerMasters;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, toSmaller)) / toLarger;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return SMALL_TO_LARGE;
            }
        }

        return NO_CHANGE;
    }

    static int calcNumReplicasStay(RepUser smallerUser, RepUser largerUser, Map<Integer, Set<Integer>> replicas) {
        Integer smallerPartitionId = smallerUser.getBasePid();
        Integer largerPartitionId = largerUser.getBasePid();
        boolean largerReplicaExistsOnSmallerMaster = largerUser.getReplicaPids().contains(smallerPartitionId);
        boolean smallerReplicaExistsOnLargerMaster = smallerUser.getReplicaPids().contains(largerPartitionId);
        int deltaReplicas = (largerReplicaExistsOnSmallerMaster ? 0 : 1) + (smallerReplicaExistsOnLargerMaster ? 0 : 1);
        int curReplicas = replicas.get(smallerPartitionId).size() + replicas.get(largerPartitionId).size();
        return curReplicas + deltaReplicas;
    }

    static int calcNumReplicasMove(RepUser movingUser, RepUser stayingUser, Map<Integer, Set<Integer>> replicas, int minNumReplicas, Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> uidToReplicasMap, Map<Integer, Set<Integer>> friendships) {
        int curReplicas = replicas.get(movingUser.getBasePid()).size() + replicas.get(stayingUser.getBasePid()).size();

        //Find replicas that need to be added
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, uidToPidMap);
        Set<Integer> replicasToAddInStayingPartition = findReplicasToAddToTargetPartition(movingUser, stayingUser.getBasePid(), uidToPidMap, uidToReplicasMap);

        //Find replicas that should be deleted
        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser, minNumReplicas, uidToPidMap);
        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser, minNumReplicas, uidToPidMap);
        Set<Integer> replicasInMovingPartitionToDelete = findReplicasInMovingPartitionToDelete(movingUser, replicasToAddInStayingPartition, minNumReplicas, uidToReplicasMap, uidToPidMap, friendships);

        //Calculate net change
        int numReplicasToAdd = replicasToAddInStayingPartition.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        int numReplicasToDelete = replicasInMovingPartitionToDelete.size() + (shouldWeDeleteReplicaOfMovingUserInStayingPartition ? 1 : 0) + (shouldWeDeleteReplicaOfStayingUserInMovingPartition ? 1 : 0);

        int deltaReplicas = numReplicasToAdd - numReplicasToDelete;

        return curReplicas + deltaReplicas;
    }

    public static Set<Integer> findReplicasToAddToTargetPartition(RepUser movingUser, Integer targetPartitionId, Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> uidToReplicasMap) {
        Set<Integer> replicasToAddInStayingPartition = new HashSet<>();
        for (Integer friendId : movingUser.getFriendIDs()) {
            int pid = uidToPidMap.get(friendId);
            Set<Integer> replicaPids = uidToReplicasMap.get(friendId);
            if (targetPartitionId != pid && !replicaPids.contains(targetPartitionId)) {
                replicasToAddInStayingPartition.add(friendId);
            }
        }

        return replicasToAddInStayingPartition;
    }

    public static Set<Integer> findReplicasInMovingPartitionToDelete(RepUser movingUser, Set<Integer> replicasToBeAdded, int minNumReplicas, Map<Integer, Set<Integer>> uidToReplicasMap, Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> friendships) {
        Set<Integer> replicasInMovingPartitionToDelete = new HashSet<>();
        for (Integer replicaId : findReplicasInPartitionThatWereOnlyThereForThisUsersSake(movingUser, uidToPidMap, friendships)) {
            int numExistingReplicas = uidToReplicasMap.get(replicaId).size();
            if (numExistingReplicas + (replicasToBeAdded.contains(replicaId) ? 1 : 0) > minNumReplicas) {
                replicasInMovingPartitionToDelete.add(replicaId);
            }
        }

        return replicasInMovingPartitionToDelete;
    }

    public static Set<Integer> findReplicasInPartitionThatWereOnlyThereForThisUsersSake(RepUser user, Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> friendships) {
        Set<Integer> replicasThatWereJustThereForThisUsersSake = new HashSet<>();
        outer:
        for (Integer friendId : user.getFriendIDs()) {
            if (!uidToPidMap.get(friendId).equals(user.getBasePid())) {
                for (Integer friendOfFriendId : friendships.get(friendId)) {
                    if (friendOfFriendId.equals(user.getId())) {
                        continue;
                    }

                    if (uidToPidMap.get(friendOfFriendId).equals(user.getBasePid())) {
                        continue outer;
                    }
                }

                replicasThatWereJustThereForThisUsersSake.add(friendId);
            }
        }

        return replicasThatWereJustThereForThisUsersSake;
    }

    static boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(RepUser movingUser, Map<Integer, Integer> uidToPidMap) {
        for (Integer friendId : movingUser.getFriendIDs()) {
            if (movingUser.getBasePid().equals(uidToPidMap.get(friendId))) {
                return true;
            }
        }

        return false;
    }

    static boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition(RepUser movingUser, RepUser stayingUser, int minNumReplicas, Map<Integer, Integer> uidToPidMap) {
        boolean couldWeDeleteReplicaOfMovingUserInStayingPartition = movingUser.getReplicaPids().contains(stayingUser.getBasePid());
        int numReplicas = movingUser.getReplicaPids().size();
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, uidToPidMap);
        int redundancyOfMovingUser = numReplicas + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        return couldWeDeleteReplicaOfMovingUserInStayingPartition && redundancyOfMovingUser > minNumReplicas;
    }

    static boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser, int minNumReplicas, Map<Integer, Integer> uidToPidMap) {
        boolean couldWeDeleteReplicaOfStayingUserInMovingPartition = couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser, uidToPidMap);
        int redundancyOfStayingUser = stayingUser.getReplicaPids().size();
        return couldWeDeleteReplicaOfStayingUserInMovingPartition && redundancyOfStayingUser > minNumReplicas;
    }

    static boolean couldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser, Map<Integer, Integer> uidToPidMap) {
        boolean movingPartitionHasStayingUserReplica = stayingUser.getReplicaPids().contains(movingUser.getBasePid());
        boolean stayingUserHasNoOtherFriendMastersInMovingPartition = true; //deleting staying user replica is a possibility, if it exists, subject to balance constraints
        for (int friendId : stayingUser.getFriendIDs()) {
            boolean friendIsNotMovingUser = friendId != movingUser.getId();
            boolean friendsMasterIsOnPartition = uidToPidMap.get(friendId).equals(movingUser.getBasePid());
            if (friendIsNotMovingUser && friendsMasterIsOnPartition) {
                stayingUserHasNoOtherFriendMastersInMovingPartition = false;
            }
        }

        return movingPartitionHasStayingUserReplica && stayingUserHasNoOtherFriendMastersInMovingPartition;
    }
}