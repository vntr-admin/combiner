package io.vntr.sparmes;

import java.util.HashSet;
import java.util.Set;

import static io.vntr.sparmes.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesBefriendingStrategy {
    private SparmesManager manager;

    public SparmesBefriendingStrategy(SparmesManager manager) {
        this.manager = manager;
    }

    public BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(SparmesUser smallerUser, SparmesUser largerUser) {
        //calculate the number of replicas that would be generated for each of the three possible configurations:
        //  1) no movements of masters, which maintains the status-quo
        //	2) the master of smallerUserId goes to the partition containing the master of largerUserId
        //  3) the opposite of (3)

        int stay      = calcNumReplicasStay(smallerUser, largerUser);
        int toLarger  = calcNumReplicasMove(smallerUser, largerUser);
        int toSmaller = calcNumReplicasMove(largerUser, smallerUser);

        int smallerMasters = manager.getPartitionById(smallerUser.getMasterPartitionId()).getNumMasters();
        int largerMasters  = manager.getPartitionById(largerUser.getMasterPartitionId()).getNumMasters();

        return determineStrategy(stay, toSmaller, toLarger, smallerMasters, largerMasters);
    }

    static BEFRIEND_REBALANCE_STRATEGY determineStrategy(int stay, int toSmaller, int toLarger, int smallerMasters, int largerMasters) {
        if (stay <= toSmaller && stay <= toLarger) {
            return NO_CHANGE;
        }

        if (toSmaller <= stay && toSmaller <= toLarger) {
            if (smallerMasters < largerMasters) {
                return LARGE_TO_SMALL;
            }

            float imbalanceRatio = (smallerMasters + 1f) / largerMasters;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, toLarger)) / toSmaller;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return LARGE_TO_SMALL;
            }
        }

        if (toLarger <= stay && toLarger <= toSmaller) {
            if (largerMasters < smallerMasters) {
                return SMALL_TO_LARGE;
            }

            float imbalanceRatio = (largerMasters + 1f) / smallerMasters;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, toSmaller)) / toLarger;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return SMALL_TO_LARGE;
            }
        }

        return NO_CHANGE;
    }

    int calcNumReplicasStay(SparmesUser smallerUser, SparmesUser largerUser) {
        Integer smallerPartitionId = smallerUser.getMasterPartitionId();
        Integer largerPartitionId = largerUser.getMasterPartitionId();
        boolean largerReplicaExistsOnSmallerMaster = largerUser.getReplicaPartitionIds().contains(smallerPartitionId);
        boolean smallerReplicaExistsOnLargerMaster = smallerUser.getReplicaPartitionIds().contains(largerPartitionId);
        int deltaReplicas = (largerReplicaExistsOnSmallerMaster ? 0 : 1) + (smallerReplicaExistsOnLargerMaster ? 0 : 1);
        int curReplicas = manager.getPartitionById(smallerPartitionId).getNumReplicas() + manager.getPartitionById(largerPartitionId).getNumReplicas();
        return curReplicas + deltaReplicas;
    }

    int calcNumReplicasMove(SparmesUser movingUser, SparmesUser stayingUser) {
        //Find replicas that need to be added
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser);
        Set<Integer> replicasToAddInStayingPartition = findReplicasToAddToTargetPartition(movingUser, stayingUser.getMasterPartitionId());

        //Find replicas that should be deleted
        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser);
        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser);
        Set<Integer> replicasInMovingPartitionToDelete = findReplicasInMovingPartitionToDelete(movingUser, replicasToAddInStayingPartition);

        //Calculate net change
        int numReplicasToAdd = replicasToAddInStayingPartition.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        int numReplicasToDelete = replicasInMovingPartitionToDelete.size() + (shouldWeDeleteReplicaOfMovingUserInStayingPartition ? 1 : 0) + (shouldWeDeleteReplicaOfStayingUserInMovingPartition ? 1 : 0);
        int deltaReplicas = numReplicasToAdd - numReplicasToDelete;
        int curReplicas = manager.getPartitionById(movingUser.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(stayingUser.getMasterPartitionId()).getNumReplicas();
        return curReplicas + deltaReplicas;
    }

    Set<Integer> findReplicasToAddToTargetPartition(SparmesUser movingUser, Integer targetPartitionId) {
        Set<Integer> replicasToAddInStayingPartition = new HashSet<Integer>();
        for (Integer friendId : movingUser.getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (!targetPartitionId.equals(friend.getMasterPartitionId()) && !friend.getReplicaPartitionIds().contains(targetPartitionId)) {
                replicasToAddInStayingPartition.add(friendId);
            }
        }

        return replicasToAddInStayingPartition;
    }

    Set<Integer> findReplicasInMovingPartitionToDelete(SparmesUser movingUser, Set<Integer> replicasToBeAdded) {
        Set<Integer> replicasInMovingPartitionToDelete = new HashSet<Integer>();
        for (Integer replicaId : findReplicasInPartitionThatWereOnlyThereForThisUsersSake(movingUser)) {
            int numExistingReplicas = manager.getUserMasterById(replicaId).getReplicaPartitionIds().size();
            if (numExistingReplicas + (replicasToBeAdded.contains(replicaId) ? 1 : 0) > manager.getMinNumReplicas()) {
                replicasInMovingPartitionToDelete.add(replicaId);
            }
        }

        return replicasInMovingPartitionToDelete;
    }

    Set<Integer> findReplicasInPartitionThatWereOnlyThereForThisUsersSake(SparmesUser user) {
        Set<Integer> replicasThatWereJustThereForThisUsersSake = new HashSet<Integer>();
        outer:
        for (Integer friendId : user.getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (!friend.getMasterPartitionId().equals(user.getMasterPartitionId())) {
                for (Integer friendOfFriendId : friend.getFriendIDs()) {
                    if (friendOfFriendId.equals(user.getId())) {
                        continue;
                    }

                    SparmesUser friendOfFriend = manager.getUserMasterById(friendOfFriendId);
                    if (friendOfFriend.getMasterPartitionId().equals(user.getMasterPartitionId())) {
                        continue outer;
                    }
                }

                replicasThatWereJustThereForThisUsersSake.add(friendId);
            }
        }

        return replicasThatWereJustThereForThisUsersSake;
    }

    boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(SparmesUser movingUser) {
        for (Integer friendId : movingUser.getFriendIDs()) {
            Integer friendMasterPartitionId = manager.getUserMasterById(friendId).getMasterPartitionId();
            if (movingUser.getMasterPartitionId().equals(friendMasterPartitionId)) {
                return true;
            }
        }

        return false;
    }

    boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition(SparmesUser movingUser, SparmesUser stayingUser) {
        boolean couldWeDeleteReplicaOfMovingUserInStayingPartition = movingUser.getReplicaPartitionIds().contains(stayingUser.getMasterPartitionId());
        int redundancyOfMovingUser = movingUser.getReplicaPartitionIds().size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser) ? 1 : 0);
        return couldWeDeleteReplicaOfMovingUserInStayingPartition && redundancyOfMovingUser > manager.getMinNumReplicas();
    }

    boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition(SparmesUser movingUser, SparmesUser stayingUser) {
        boolean couldWeDeleteReplicaOfStayingUserInMovingPartition = couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser);
        int redundancyOfStayingUser = stayingUser.getReplicaPartitionIds().size();
        return couldWeDeleteReplicaOfStayingUserInMovingPartition && redundancyOfStayingUser > manager.getMinNumReplicas();
    }

    boolean couldWeDeleteReplicaOfStayingUserInMovingPartition(SparmesUser movingUser, SparmesUser stayingUser) {
        boolean movingPartitionHasStayingUserReplica = stayingUser.getReplicaPartitionIds().contains(movingUser.getMasterPartitionId());
        boolean stayingUserHasNoOtherFriendMastersInMovingPartition = true; //deleting staying user replica is a possibility, if it exists, subject to balance constraints
        for (Integer friendId : stayingUser.getFriendIDs()) {
            Integer friendMasterPartitionId = manager.getUserMasterById(friendId).getMasterPartitionId();
            if (!(friendId.equals(movingUser.getId())) && friendMasterPartitionId.equals(movingUser.getMasterPartitionId())) {
                stayingUserHasNoOtherFriendMastersInMovingPartition = false;
            }
        }

        return movingPartitionHasStayingUserReplica && stayingUserHasNoOtherFriendMastersInMovingPartition;
    }
}
