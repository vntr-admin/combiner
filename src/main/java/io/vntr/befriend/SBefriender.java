package io.vntr.befriend;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;

import static io.vntr.utils.TroveUtils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

public class SBefriender {
    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(RepUser smallerUser, RepUser largerUser, int minNumReplicas, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas) {
        //calculate the number of replicas that would be generated for each of the three possible configurations:
        //  1) no movements of masters, which maintains the status-quo
        //	2) the master of smallerUserId goes to the partition containing the master of largerUserId
        //  3) the opposite of (3)

        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

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

            float imbalanceRatio = ((float) smallerMasters) / largerMasters;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, toLarger)) / toSmaller;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return LARGE_TO_SMALL;
            }
        }

        if (toLarger <= stay && toLarger <= toSmaller) {
            if (largerMasters < smallerMasters) {
                return SMALL_TO_LARGE;
            }

            float imbalanceRatio = ((float) largerMasters) / smallerMasters;
            float ratioOfSecondBestToBest = ((float) Math.min(stay, toSmaller)) / toLarger;
            if (ratioOfSecondBestToBest > imbalanceRatio) {
                return SMALL_TO_LARGE;
            }
        }

        return NO_CHANGE;
    }

    static int calcNumReplicasStay(RepUser smallerUser, RepUser largerUser, TIntObjectMap<TIntSet> replicas) {
        Integer smallerPartitionId = smallerUser.getBasePid();
        Integer largerPartitionId = largerUser.getBasePid();
        boolean largerReplicaExistsOnSmallerMaster = largerUser.getReplicaPids().contains(smallerPartitionId);
        boolean smallerReplicaExistsOnLargerMaster = smallerUser.getReplicaPids().contains(largerPartitionId);
        int deltaReplicas = (largerReplicaExistsOnSmallerMaster ? 0 : 1) + (smallerReplicaExistsOnLargerMaster ? 0 : 1);
        int curReplicas = replicas.get(smallerPartitionId).size() + replicas.get(largerPartitionId).size();
        return curReplicas + deltaReplicas;
    }

    static int calcNumReplicasMove(RepUser movingUser, RepUser stayingUser, TIntObjectMap<TIntSet> replicas, int minNumReplicas, TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> uidToReplicasMap, TIntObjectMap<TIntSet> friendships) {
        int curReplicas = replicas.get(movingUser.getBasePid()).size() + replicas.get(stayingUser.getBasePid()).size();

        //Find replicas that need to be added
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, uidToPidMap);
        TIntSet replicasToAddInStayingPartition = findReplicasToAddToTargetPartition(movingUser, stayingUser.getBasePid(), uidToPidMap, uidToReplicasMap);

        //Find replicas that should be deleted
        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser, minNumReplicas, uidToPidMap);
        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser, minNumReplicas, uidToPidMap);
        TIntSet replicasInMovingPartitionToDelete = findReplicasInMovingPartitionToDelete(movingUser, replicasToAddInStayingPartition, minNumReplicas, uidToReplicasMap, uidToPidMap, friendships);

        //Calculate net change
        int numReplicasToAdd = replicasToAddInStayingPartition.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        int numReplicasToDelete = replicasInMovingPartitionToDelete.size() + (shouldWeDeleteReplicaOfMovingUserInStayingPartition ? 1 : 0) + (shouldWeDeleteReplicaOfStayingUserInMovingPartition ? 1 : 0);

        int deltaReplicas = numReplicasToAdd - numReplicasToDelete;

        return curReplicas + deltaReplicas;
    }

    public static TIntSet findReplicasToAddToTargetPartition(RepUser movingUser, Integer targetPartitionId, TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> uidToReplicasMap) {
        TIntSet replicasToAddInStayingPartition = new TIntHashSet();
        for(TIntIterator iter = movingUser.getFriendIDs().iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            int pid = uidToPidMap.get(friendId);
            TIntSet replicaPids = uidToReplicasMap.get(friendId);
            if (targetPartitionId != pid && !replicaPids.contains(targetPartitionId)) {
                replicasToAddInStayingPartition.add(friendId);
            }
        }

        return replicasToAddInStayingPartition;
    }

    public static TIntSet findReplicasInMovingPartitionToDelete(RepUser movingUser, TIntSet replicasToBeAdded, int minNumReplicas, TIntObjectMap<TIntSet> uidToReplicasMap, TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> friendships) {
        TIntSet replicasInMovingPartitionToDelete = new TIntHashSet();

        for(TIntIterator iter = findReplicasInPartitionThatWereOnlyThereForThisUsersSake(movingUser, uidToPidMap, friendships).iterator(); iter.hasNext(); ) {
            int replicaId = iter.next();
            int numExistingReplicas = uidToReplicasMap.get(replicaId).size();
            if (numExistingReplicas + (replicasToBeAdded.contains(replicaId) ? 1 : 0) > minNumReplicas) {
                replicasInMovingPartitionToDelete.add(replicaId);
            }
        }

        return replicasInMovingPartitionToDelete;
    }

    public static TIntSet findReplicasInPartitionThatWereOnlyThereForThisUsersSake(RepUser user, TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> friendships) {
        TIntSet replicasThatWereJustThereForThisUsersSake = new TIntHashSet();
        outer: for(TIntIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            if (uidToPidMap.get(friendId) != user.getBasePid()) {
                for(TIntIterator iter2 = friendships.get(friendId).iterator(); iter2.hasNext(); ) {
                    int friendOfFriendId = iter2.next();
                    if (friendOfFriendId == user.getId()) {
                        continue;
                    }

                    if (uidToPidMap.get(friendOfFriendId) == user.getBasePid()) {
                        continue outer;
                    }
                }

                replicasThatWereJustThereForThisUsersSake.add(friendId);
            }
        }

        return replicasThatWereJustThereForThisUsersSake;
    }

    static boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(RepUser movingUser, TIntIntMap uidToPidMap) {
        for(TIntIterator iter = movingUser.getFriendIDs().iterator(); iter.hasNext(); ) {
            if (movingUser.getBasePid().equals(uidToPidMap.get(iter.next()))) {
                return true;
            }
        }

        return false;
    }

    static boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition(RepUser movingUser, RepUser stayingUser, int minNumReplicas, TIntIntMap uidToPidMap) {
        boolean couldWeDeleteReplicaOfMovingUserInStayingPartition = movingUser.getReplicaPids().contains(stayingUser.getBasePid());
        int numReplicas = movingUser.getReplicaPids().size();
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, uidToPidMap);
        int redundancyOfMovingUser = numReplicas + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        return couldWeDeleteReplicaOfMovingUserInStayingPartition && redundancyOfMovingUser > minNumReplicas;
    }

    static boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser, int minNumReplicas, TIntIntMap uidToPidMap) {
        boolean couldWeDeleteReplicaOfStayingUserInMovingPartition = couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser, uidToPidMap);
        int redundancyOfStayingUser = stayingUser.getReplicaPids().size();
        return couldWeDeleteReplicaOfStayingUserInMovingPartition && redundancyOfStayingUser > minNumReplicas;
    }

    static boolean couldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser, TIntIntMap uidToPidMap) {
        boolean movingPartitionHasStayingUserReplica = stayingUser.getReplicaPids().contains(movingUser.getBasePid());
        boolean stayingUserHasNoOtherFriendMastersInMovingPartition = true; //deleting staying user replica is a possibility, if it exists, subject to balance constraints
        for(TIntIterator iter = stayingUser.getFriendIDs().iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            boolean friendIsNotMovingUser = friendId != movingUser.getId();
            boolean friendsMasterIsOnPartition = uidToPidMap.get(friendId) == movingUser.getBasePid();
            if (friendIsNotMovingUser && friendsMasterIsOnPartition) {
                stayingUserHasNoOtherFriendMastersInMovingPartition = false;
            }
        }

        return movingPartitionHasStayingUserReplica && stayingUserHasNoOtherFriendMastersInMovingPartition;
    }
}