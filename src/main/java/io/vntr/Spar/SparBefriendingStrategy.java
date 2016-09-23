package io.vntr.spar;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SparBefriendingStrategy {
    private SparManager manager;

    public SparBefriendingStrategy(SparManager manager) {
        this.manager = manager;
    }

    public BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(SparUser smallerUser, SparUser largerUser) {
        //calculate the number of replicas that would be generated for each of the three possible configurations:
        //  1) no movements of masters, which maintains the status-quo
        //	2) the master of smallerUserId goes to the partition containing the master of largerUserId
        //  3) the opposite of (3)

        int numReplicasNoMovement = calcNumReplicasNoMovement(smallerUser, largerUser);
        int numReplicasSmallerToLarger = calcNumReplicasOneMovesToOther(smallerUser, largerUser);
        int numReplicasLargerToSmaller = calcNumReplicasOneMovesToOther(largerUser, smallerUser);

        if (numReplicasNoMovement <= numReplicasLargerToSmaller && numReplicasNoMovement <= numReplicasSmallerToLarger) {
            return BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
        }

        SparPartition smallerPartition = manager.getPartitionById(smallerUser.getMasterPartitionId());
        SparPartition largerPartition = manager.getPartitionById(largerUser.getMasterPartitionId());

        int curNumMastersSmaller = smallerPartition.getNumMasters();
        int curNumMastersLarger = largerPartition.getNumMasters();

        if (numReplicasLargerToSmaller <= numReplicasNoMovement && numReplicasLargerToSmaller <= numReplicasSmallerToLarger) {
            if (curNumMastersSmaller < curNumMastersLarger) {
                return BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
            }

            double imbalanceRatio = (curNumMastersSmaller + 1D) / curNumMastersLarger;
            if (numReplicasNoMovement <= numReplicasSmallerToLarger) {
                //Second-best option is no movement
                double ratioOfNoChangeReplicasToL2SReplicas = ((double) numReplicasNoMovement) / numReplicasLargerToSmaller;
                if (ratioOfNoChangeReplicasToL2SReplicas > imbalanceRatio) {
                    return BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
                }
            } else {
                //Second-best option is smaller-to-larger
                double ratioOfS2LReplicasToL2SReplicas = ((double) numReplicasSmallerToLarger) / numReplicasLargerToSmaller;
                if (ratioOfS2LReplicasToL2SReplicas > imbalanceRatio) {
                    return BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
                }
            }
        }

        if (numReplicasSmallerToLarger <= numReplicasNoMovement && numReplicasSmallerToLarger <= numReplicasLargerToSmaller) {
            if (curNumMastersLarger < curNumMastersSmaller) {
                return BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
            }

            double imbalanceRatio = (curNumMastersLarger + 1D) / curNumMastersSmaller;

            if (numReplicasNoMovement <= numReplicasLargerToSmaller) {
                //Second-best option is no movement
                double ratioOfNoChangeReplicasToS2LReplicas = ((double) numReplicasNoMovement) / numReplicasSmallerToLarger;
                if (ratioOfNoChangeReplicasToS2LReplicas > imbalanceRatio) {
                    return BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
                }
            } else {
                //Second-best option is larger-to-smaller
                double ratioOfL2SReplicasToS2LReplicas = ((double) numReplicasLargerToSmaller) / numReplicasSmallerToLarger;
                if (ratioOfL2SReplicasToS2LReplicas > imbalanceRatio) {
                    return BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
                }
            }
        }

        return BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
    }

    int calcNumReplicasNoMovement(SparUser smallerUser, SparUser largerUser) {
        Long smallerPartitionId = smallerUser.getMasterPartitionId();
        Long largerPartitionId = largerUser.getMasterPartitionId();
        boolean largerReplicaExistsOnSmallerMaster = largerUser.getReplicaPartitionIds().contains(smallerPartitionId);
        boolean smallerReplicaExistsOnLargerMaster = smallerUser.getReplicaPartitionIds().contains(largerPartitionId);
        int deltaReplicas = (largerReplicaExistsOnSmallerMaster ? 0 : 1) + (smallerReplicaExistsOnLargerMaster ? 0 : 1);
        int curReplicas = manager.getPartitionById(smallerPartitionId).getNumReplicas() + manager.getPartitionById(largerPartitionId).getNumReplicas();
        return curReplicas + deltaReplicas;
    }

    int calcNumReplicasOneMovesToOther(SparUser movingUser, SparUser stayingUser) {
        //Find replicas that need to be added
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser);
        Set<Long> replicasToAddInStayingPartition = findReplicasToAddToTargetPartition(movingUser, stayingUser.getMasterPartitionId());

        //Find replicas that should be deleted
        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser);
        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser);
        Set<Long> replicasInMovingPartitionToDelete = findReplicasInMovingPartitionToDelete(movingUser, replicasToAddInStayingPartition);

        //Calculate net change
        int numReplicasToAdd = replicasToAddInStayingPartition.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        int numReplicasToDelete = replicasInMovingPartitionToDelete.size() + (shouldWeDeleteReplicaOfMovingUserInStayingPartition ? 1 : 0) + (shouldWeDeleteReplicaOfStayingUserInMovingPartition ? 1 : 0);
        int deltaReplicas = numReplicasToAdd - numReplicasToDelete;
        int curReplicas = manager.getPartitionById(movingUser.getMasterPartitionId()).getNumMasters() + manager.getPartitionById(stayingUser.getMasterPartitionId()).getNumMasters();
        return curReplicas + deltaReplicas;
    }

    Set<Long> findReplicasToAddToTargetPartition(SparUser movingUser, Long targetPartitionId) {
        Set<Long> replicasToAddInStayingPartition = new HashSet<Long>();
        for (Long friendId : movingUser.getFriendIDs()) {
            SparUser friend = manager.getUserMasterById(friendId);
            if (!targetPartitionId.equals(friend.getMasterPartitionId()) && !friend.getReplicaPartitionIds().contains(targetPartitionId)) {
                replicasToAddInStayingPartition.add(friendId);
            }
        }

        return replicasToAddInStayingPartition;
    }

    Set<Long> findReplicasInMovingPartitionToDelete(SparUser movingUser, Set<Long> replicasToBeAdded) {
        Set<Long> replicasInMovingPartitionToDelete = new HashSet<Long>();
        for (Long replicaId : findReplicasInPartitionThatWereOnlyThereForThisUsersSake(movingUser)) {
            int numExistingReplicas = manager.getUserMasterById(replicaId).getReplicaPartitionIds().size();
            if (numExistingReplicas + (replicasToBeAdded.contains(replicaId) ? 1 : 0) > manager.getMinNumReplicas()) {
                replicasInMovingPartitionToDelete.add(replicaId);
            }
        }

        return replicasInMovingPartitionToDelete;
    }

    Set<Long> findReplicasInPartitionThatWereOnlyThereForThisUsersSake(SparUser user) {
        Set<Long> replicasThatWereJustThereForThisUsersSake = new HashSet<Long>();
        outer:
        for (Long friendId : user.getFriendIDs()) {
            SparUser friend = manager.getUserMasterById(friendId);
            if (!friend.getMasterPartitionId().equals(user.getMasterPartitionId())) {
                for (Long friendOfFriendId : friend.getFriendIDs()) {
                    if (friendOfFriendId.equals(user.getId())) {
                        continue;
                    }

                    SparUser friendOfFriend = manager.getUserMasterById(friendOfFriendId);
                    if (friendOfFriend.getMasterPartitionId().equals(user.getMasterPartitionId())) {
                        continue outer;
                    }
                }

                replicasThatWereJustThereForThisUsersSake.add(friendId);
            }
        }

        return replicasThatWereJustThereForThisUsersSake;
    }

    private boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(SparUser movingUser) {
        for (Long friendId : movingUser.getFriendIDs()) {
            Long friendMasterPartitionId = manager.getUserMasterById(friendId).getMasterPartitionId();
            if (movingUser.getMasterPartitionId().equals(friendMasterPartitionId)) {
                return true;
            }
        }

        return false;
    }

    boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition(SparUser movingUser, SparUser stayingUser) {
        boolean couldWeDeleteReplicaOfMovingUserInStayingPartition = movingUser.getReplicaPartitionIds().contains(stayingUser.getMasterPartitionId());
        int redundancyOfMovingUser = movingUser.getReplicaPartitionIds().size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser) ? 1 : 0);
        return couldWeDeleteReplicaOfMovingUserInStayingPartition && redundancyOfMovingUser > manager.getMinNumReplicas();
    }

    boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition(SparUser movingUser, SparUser stayingUser) {
        boolean couldWeDeleteReplicaOfStayingUserInMovingPartition = couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser);
        int redundancyOfStayingUser = stayingUser.getReplicaPartitionIds().size();
        return couldWeDeleteReplicaOfStayingUserInMovingPartition && redundancyOfStayingUser > manager.getMinNumReplicas();
    }

    boolean couldWeDeleteReplicaOfStayingUserInMovingPartition(SparUser movingUser, SparUser stayingUser) {
        boolean movingPartitionHasStayingUserReplica = stayingUser.getReplicaPartitionIds().contains(movingUser.getMasterPartitionId());
        boolean stayingUserHasNoOtherFriendMastersInMovingPartition = true; //deleting staying user replica is a possibility, if it exists, subject to balance constraints
        for (Long friendId : stayingUser.getFriendIDs()) {
            Long friendMasterPartitionId = manager.getUserMasterById(friendId).getMasterPartitionId();
            if (!(friendId.equals(movingUser.getId())) && friendMasterPartitionId.equals(movingUser.getMasterPartitionId())) {
                stayingUserHasNoOtherFriendMastersInMovingPartition = false;
            }
        }

        return movingPartitionHasStayingUserReplica && stayingUserHasNoOtherFriendMastersInMovingPartition;
    }
}