package io.vntr.spaja;

import java.util.HashSet;
import java.util.Set;

import static io.vntr.spaja.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaBefriendingStrategy {
    private SpajaManager manager;

    public SpajaBefriendingStrategy(SpajaManager manager) {
        this.manager = manager;
    }

    public BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(SpajaUser smallerUser, SpajaUser largerUser) {
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

    int calcNumReplicasStay(SpajaUser smallerUser, SpajaUser largerUser) {
        Integer smallerPartitionId = smallerUser.getMasterPartitionId();
        Integer largerPartitionId = largerUser.getMasterPartitionId();
        boolean largerReplicaExistsOnSmallerMaster = largerUser.getReplicaPartitionIds().contains(smallerPartitionId);
        boolean smallerReplicaExistsOnLargerMaster = smallerUser.getReplicaPartitionIds().contains(largerPartitionId);
        int deltaReplicas = (largerReplicaExistsOnSmallerMaster ? 0 : 1) + (smallerReplicaExistsOnLargerMaster ? 0 : 1);
        int curReplicas = manager.getPartitionById(smallerPartitionId).getNumReplicas() + manager.getPartitionById(largerPartitionId).getNumReplicas();
        return curReplicas + deltaReplicas;
    }

    int calcNumReplicasMove(SpajaUser movingUser, SpajaUser stayingUser) {
        //Find replicas that need to be added
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, stayingUser.getMasterPartitionId());
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

    Set<Integer> findReplicasToAddToTargetPartition(SpajaUser movingUser, Integer targetPartitionId) {
        Set<Integer> replicasToAddInStayingPartition = new HashSet<Integer>();
        for (Integer friendId : movingUser.getFriendIDs()) {
            SpajaUser friend = manager.getUserMasterById(friendId);
            if (!targetPartitionId.equals(friend.getMasterPartitionId()) && !friend.getReplicaPartitionIds().contains(targetPartitionId)) {
                replicasToAddInStayingPartition.add(friendId);
            }
        }

        return replicasToAddInStayingPartition;
    }

    boolean shouldDeleteReplicaInTargetPartition(SpajaUser user, Integer targetPid, int k) {
        boolean answer = false;
        if(user.getReplicaPartitionIds().contains(targetPid)) {
            boolean addReplicaInCurrentPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(user, targetPid);
            int numReplicas = user.getReplicaPartitionIds().size() + (addReplicaInCurrentPartition ? 1 : 0);
            answer = numReplicas > k;
        }
        return answer;
    }

    Set<Integer> findReplicasInMovingPartitionToDelete(SpajaUser movingUser, Set<Integer> replicasToBeAdded) {
        Set<Integer> replicasInMovingPartitionToDelete = new HashSet<Integer>();
        for (Integer replicaId : findReplicasInPartitionThatWereOnlyThereForThisUsersSake(movingUser)) {
            int numExistingReplicas = manager.getUserMasterById(replicaId).getReplicaPartitionIds().size();
            if (numExistingReplicas + (replicasToBeAdded.contains(replicaId) ? 1 : 0) > manager.getMinNumReplicas()) {
                replicasInMovingPartitionToDelete.add(replicaId);
            }
        }

        return replicasInMovingPartitionToDelete;
    }

    Set<Integer> findReplicasInPartitionThatWereOnlyThereForThisUsersSake(SpajaUser user) {
        Set<Integer> replicasThatWereJustThereForThisUsersSake = new HashSet<Integer>();
        outer:
        for (Integer friendId : user.getFriendIDs()) {
            SpajaUser friend = manager.getUserMasterById(friendId);
            if (!friend.getMasterPartitionId().equals(user.getMasterPartitionId())) {
                for (Integer friendOfFriendId : friend.getFriendIDs()) {
                    if (friendOfFriendId.equals(user.getId())) {
                        continue;
                    }

                    SpajaUser friendOfFriend = manager.getUserMasterById(friendOfFriendId);
                    if (friendOfFriend.getMasterPartitionId().equals(user.getMasterPartitionId())) {
                        continue outer;
                    }
                }

                replicasThatWereJustThereForThisUsersSake.add(friendId);
            }
        }

        return replicasThatWereJustThereForThisUsersSake;
    }

    boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(SpajaUser movingUser, int targetPid) {
        for (Integer friendId : movingUser.getFriendIDs()) {
            Integer friendMasterPartitionId = manager.getUserMasterById(friendId).getMasterPartitionId();
            if (movingUser.getMasterPartitionId().equals(friendMasterPartitionId)) {
                return true;
            }
        }
        Set<Integer> replicas = movingUser.getReplicaPartitionIds();
        if(replicas.size() <= manager.getMinNumReplicas() && replicas.contains(targetPid)) {
            return true;
        }

        return false;
    }

    boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition(SpajaUser movingUser, SpajaUser stayingUser) {
        int targetPid = stayingUser.getMasterPartitionId();
        Set<Integer> moversReplicas = movingUser.getReplicaPartitionIds();
        boolean couldWeDeleteReplicaOfMovingUserInStayingPartition = moversReplicas.contains(targetPid);
        int redundancyOfMovingUser = moversReplicas.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, targetPid) ? 1 : 0);
        return couldWeDeleteReplicaOfMovingUserInStayingPartition && redundancyOfMovingUser > manager.getMinNumReplicas();
    }

    boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition(SpajaUser movingUser, SpajaUser stayingUser) {
        boolean couldWeDeleteReplicaOfStayingUserInMovingPartition = couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser);
        int redundancyOfStayingUser = stayingUser.getReplicaPartitionIds().size();
        return couldWeDeleteReplicaOfStayingUserInMovingPartition && redundancyOfStayingUser > manager.getMinNumReplicas();
    }

    boolean couldWeDeleteReplicaOfStayingUserInMovingPartition(SpajaUser movingUser, SpajaUser stayingUser) {
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

    public SwapChanges getSwapChanges(SpajaUser u1, SpajaUser u2) {
        boolean u1AndU2AreFriends = u1.getFriendIDs().contains(u2.getId());
        int k = manager.getMinNumReplicas();
        Integer pid1 = u1.getMasterPartitionId();
        Integer pid2 = u2.getMasterPartitionId();
        boolean u1HasReplicaOnPid2 = u1.getReplicaPartitionIds().contains(pid2);
        boolean u2HasReplicaOnPid1 = u2.getReplicaPartitionIds().contains(pid1);

        Set<Integer>  mutualFriends = new HashSet<Integer>(u1.getFriendIDs());
        mutualFriends.retainAll(u2.getFriendIDs());

        SwapChanges swapChanges = new SwapChanges();
        swapChanges.setPid1(pid1);
        swapChanges.setPid2(pid2);

        Set<Integer> addToP1 = findReplicasToAddToTargetPartition(u2, pid1);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(u1, pid2)) {
            addToP1.add(u1.getId());
        }
        swapChanges.setAddToP1(addToP1);

        Set<Integer> addToP2 = findReplicasToAddToTargetPartition(u1, pid2);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(u2, pid1)) {
            addToP2.add(u2.getId());
        }
        swapChanges.setAddToP2(addToP2);

        Set<Integer> removeFromP1 = findReplicasInMovingPartitionToDelete(u1, addToP1);
        if(shouldDeleteReplicaInTargetPartition(u2, pid1, k)) {
            removeFromP1.add(u2.getId());
        }
        removeFromP1.removeAll(mutualFriends);
        swapChanges.setRemoveFromP1(removeFromP1);

        Set<Integer> removeFromP2 = findReplicasInMovingPartitionToDelete(u2, addToP2);
        if(shouldDeleteReplicaInTargetPartition(u1, pid2, k)) {
            removeFromP2.add(u1.getId());
        }
        removeFromP2.removeAll(mutualFriends);
        swapChanges.setRemoveFromP2(removeFromP2);

        return swapChanges;
    }
}
