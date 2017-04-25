//package io.vntr.sparmes;
//
//import io.vntr.RepUser;
//
//import java.util.HashSet;
//import java.util.Set;
//
//import static io.vntr.sparmes.BEFRIEND_REBALANCE_STRATEGY.*;
//
///**
// * Created by robertlindquist on 9/28/16.
// */
//public class SparmesBefriendingStrategy {
//    private SparmesManager manager;
//
//    public SparmesBefriendingStrategy(SparmesManager manager) {
//        this.manager = manager;
//    }
//
//    public BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(RepUser smallerUser, RepUser largerUser) {
//        //calculate the number of replicas that would be generated for each of the three possible configurations:
//        //  1) no movements of masters, which maintains the status-quo
//        //	2) the master of smallerUserId goes to the partition containing the master of largerUserId
//        //  3) the opposite of (3)
//
//        int stay      = calcNumReplicasStay(smallerUser, largerUser);
//        int toLarger  = calcNumReplicasMove(smallerUser, largerUser);
//        int toSmaller = calcNumReplicasMove(largerUser, smallerUser);
//
//        int smallerMasters = manager.getPartitionById(smallerUser.getBasePid()).getNumMasters();
//        int largerMasters  = manager.getPartitionById(largerUser.getBasePid()).getNumMasters();
//
//        return determineStrategy(stay, toSmaller, toLarger, smallerMasters, largerMasters);
//    }
//
//    static BEFRIEND_REBALANCE_STRATEGY determineStrategy(int stay, int toSmaller, int toLarger, int smallerMasters, int largerMasters) {
//        if (stay <= toSmaller && stay <= toLarger) {
//            return NO_CHANGE;
//        }
//
//        if (toSmaller <= stay && toSmaller <= toLarger) {
//            if (smallerMasters < largerMasters) {
//                return LARGE_TO_SMALL;
//            }
//
//            float imbalanceRatio = (smallerMasters + 1f) / largerMasters;
//            float ratioOfSecondBestToBest = ((float) Math.min(stay, toLarger)) / toSmaller;
//            if (ratioOfSecondBestToBest > imbalanceRatio) {
//                return LARGE_TO_SMALL;
//            }
//        }
//
//        if (toLarger <= stay && toLarger <= toSmaller) {
//            if (largerMasters < smallerMasters) {
//                return SMALL_TO_LARGE;
//            }
//
//            float imbalanceRatio = (largerMasters + 1f) / smallerMasters;
//            float ratioOfSecondBestToBest = ((float) Math.min(stay, toSmaller)) / toLarger;
//            if (ratioOfSecondBestToBest > imbalanceRatio) {
//                return SMALL_TO_LARGE;
//            }
//        }
//
//        return NO_CHANGE;
//    }
//
//    int calcNumReplicasStay(RepUser smallerUser, RepUser largerUser) {
//        Integer smallerPartitionId = smallerUser.getBasePid();
//        Integer largerPartitionId = largerUser.getBasePid();
//        boolean largerReplicaExistsOnSmallerMaster = largerUser.getReplicaPids().contains(smallerPartitionId);
//        boolean smallerReplicaExistsOnLargerMaster = smallerUser.getReplicaPids().contains(largerPartitionId);
//        int deltaReplicas = (largerReplicaExistsOnSmallerMaster ? 0 : 1) + (smallerReplicaExistsOnLargerMaster ? 0 : 1);
//        int curReplicas = manager.getPartitionById(smallerPartitionId).getNumReplicas() + manager.getPartitionById(largerPartitionId).getNumReplicas();
//        return curReplicas + deltaReplicas;
//    }
//
//    int calcNumReplicasMove(RepUser movingUser, RepUser stayingUser) {
//        //Find replicas that need to be added
//        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, stayingUser.getBasePid());
//        Set<Integer> replicasToAddInStayingPartition = findReplicasToAddToTargetPartition(movingUser, stayingUser.getBasePid());
//
//        //Find replicas that should be deleted
//        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser);
//        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser);
//        Set<Integer> replicasInMovingPartitionToDelete = findReplicasInMovingPartitionToDelete(movingUser, replicasToAddInStayingPartition);
//
//        //Calculate net change
//        int numReplicasToAdd = replicasToAddInStayingPartition.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
//        int numReplicasToDelete = replicasInMovingPartitionToDelete.size() + (shouldWeDeleteReplicaOfMovingUserInStayingPartition ? 1 : 0) + (shouldWeDeleteReplicaOfStayingUserInMovingPartition ? 1 : 0);
//        int deltaReplicas = numReplicasToAdd - numReplicasToDelete;
//        int curReplicas = manager.getPartitionById(movingUser.getBasePid()).getNumReplicas() + manager.getPartitionById(stayingUser.getBasePid()).getNumReplicas();
//        return curReplicas + deltaReplicas;
//    }
//
//    Set<Integer> findReplicasToAddToTargetPartition(RepUser movingUser, Integer targetPartitionId) {
//        Set<Integer> replicasToAddInStayingPartition = new HashSet<>();
//        for (Integer friendId : movingUser.getFriendIDs()) {
//            RepUser friend = manager.getUserMasterById(friendId);
//            if (!targetPartitionId.equals(friend.getBasePid()) && !friend.getReplicaPids().contains(targetPartitionId)) {
//                replicasToAddInStayingPartition.add(friendId);
//            }
//        }
//
//        return replicasToAddInStayingPartition;
//    }
//
//    Set<Integer> findReplicasInMovingPartitionToDelete(RepUser movingUser, Set<Integer> replicasToBeAdded) {
//        Set<Integer> replicasInMovingPartitionToDelete = new HashSet<>();
//        for (Integer replicaId : findReplicasInPartitionThatWereOnlyThereForThisUsersSake(movingUser)) {
//            int numExistingReplicas = manager.getUserMasterById(replicaId).getReplicaPids().size();
//            if (numExistingReplicas + (replicasToBeAdded.contains(replicaId) ? 1 : 0) > manager.getMinNumReplicas()) {
//                replicasInMovingPartitionToDelete.add(replicaId);
//            }
//        }
//
//        return replicasInMovingPartitionToDelete;
//    }
//
//    Set<Integer> findReplicasInPartitionThatWereOnlyThereForThisUsersSake(RepUser user) {
//        Set<Integer> replicasThatWereJustThereForThisUsersSake = new HashSet<>();
//        outer:
//        for (Integer friendId : user.getFriendIDs()) {
//            RepUser friend = manager.getUserMasterById(friendId);
//            if (!friend.getBasePid().equals(user.getBasePid())) {
//                for (Integer friendOfFriendId : friend.getFriendIDs()) {
//                    if (friendOfFriendId.equals(user.getId())) {
//                        continue;
//                    }
//
//                    RepUser friendOfFriend = manager.getUserMasterById(friendOfFriendId);
//                    if (friendOfFriend.getBasePid().equals(user.getBasePid())) {
//                        continue outer;
//                    }
//                }
//
//                replicasThatWereJustThereForThisUsersSake.add(friendId);
//            }
//        }
//
//        return replicasThatWereJustThereForThisUsersSake;
//    }
//
//    boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(RepUser movingUser, int targetPid) {
//        for (Integer friendId : movingUser.getFriendIDs()) {
//            Integer friendMasterPartitionId = manager.getUserMasterById(friendId).getBasePid();
//            if (movingUser.getBasePid().equals(friendMasterPartitionId)) {
//                return true;
//            }
//        }
//
//        Set<Integer> replicas = movingUser.getReplicaPids();
//        return replicas.size() <= manager.getMinNumReplicas() && replicas.contains(targetPid);
//
//    }
//
//    boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition(RepUser movingUser, RepUser stayingUser) {
//        int targetPid = stayingUser.getBasePid();
//        Set<Integer> moversReplicas = movingUser.getReplicaPids();
//        boolean couldWeDeleteReplicaOfMovingUserInStayingPartition = moversReplicas.contains(targetPid);
//        int redundancyOfMovingUser = moversReplicas.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, targetPid) ? 1 : 0);
//        return couldWeDeleteReplicaOfMovingUserInStayingPartition && redundancyOfMovingUser > manager.getMinNumReplicas();
//    }
//
//    boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser) {
//        boolean couldWeDeleteReplicaOfStayingUserInMovingPartition = couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser);
//        int redundancyOfStayingUser = stayingUser.getReplicaPids().size();
//        return couldWeDeleteReplicaOfStayingUserInMovingPartition && redundancyOfStayingUser > manager.getMinNumReplicas();
//    }
//
//    boolean couldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser) {
//        boolean movingPartitionHasStayingUserReplica = stayingUser.getReplicaPids().contains(movingUser.getBasePid());
//        boolean stayingUserHasNoOtherFriendMastersInMovingPartition = true; //deleting staying user replica is a possibility, if it exists, subject to balance constraints
//        for (Integer friendId : stayingUser.getFriendIDs()) {
//            Integer friendMasterPartitionId = manager.getUserMasterById(friendId).getBasePid();
//            if (!(friendId.equals(movingUser.getId())) && friendMasterPartitionId.equals(movingUser.getBasePid())) {
//                stayingUserHasNoOtherFriendMastersInMovingPartition = false;
//            }
//        }
//
//        return movingPartitionHasStayingUserReplica && stayingUserHasNoOtherFriendMastersInMovingPartition;
//    }
//}
