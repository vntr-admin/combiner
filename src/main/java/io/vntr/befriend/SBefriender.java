package io.vntr.befriend;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.RepUser;

import static io.vntr.utils.TroveUtils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

public class SBefriender {
    public static BEFRIEND_REBALANCE_STRATEGY determineBestBefriendingRebalanceStrategy(RepUser smallerUser, RepUser largerUser, int minNumReplicas, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas) {
        //calculate the number of replicas that would be generated for each of the three possible configurations:
        //  1) no movements of masters, which maintains the status-quo
        //	2) the master of smallerUserId goes to the partition containing the master of largerUserId
        //  3) the opposite of (3)

        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

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

    static int calcNumReplicasStay(RepUser smallerUser, RepUser largerUser, TShortObjectMap<TShortSet> replicas) {
        short smallerPid = smallerUser.getBasePid();
        short largerPid = largerUser.getBasePid();
        boolean largerReplicaExistsOnSmallerMaster = largerUser.getReplicaPids().contains(smallerPid);
        boolean smallerReplicaExistsOnLargerMaster = smallerUser.getReplicaPids().contains(largerPid);
        int deltaReplicas = (largerReplicaExistsOnSmallerMaster ? 0 : 1) + (smallerReplicaExistsOnLargerMaster ? 0 : 1);
        int curReplicas = replicas.get(smallerPid).size() + replicas.get(largerPid).size();
        return curReplicas + deltaReplicas;
    }

    static int calcNumReplicasMove(RepUser movingUser, RepUser stayingUser, TShortObjectMap<TShortSet> replicas, int minNumReplicas, TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> uidToReplicasMap, TShortObjectMap<TShortSet> friendships) {
        int curReplicas = replicas.get(movingUser.getBasePid()).size() + replicas.get(stayingUser.getBasePid()).size();

        //Find replicas that need to be added
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, uidToPidMap);
        TShortSet replicasToAddInStayingPartition = findReplicasToAddToTargetPartition(movingUser, stayingUser.getBasePid(), uidToPidMap, uidToReplicasMap);

        //Find replicas that should be deleted
        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser, minNumReplicas, uidToPidMap);
        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser, minNumReplicas, uidToPidMap);
        TShortSet replicasInMovingPartitionToDelete = findReplicasInMovingPartitionToDelete(movingUser, replicasToAddInStayingPartition, minNumReplicas, uidToReplicasMap, uidToPidMap, friendships);

        //Calculate net change
        int numReplicasToAdd = replicasToAddInStayingPartition.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        int numReplicasToDelete = replicasInMovingPartitionToDelete.size() + (shouldWeDeleteReplicaOfMovingUserInStayingPartition ? 1 : 0) + (shouldWeDeleteReplicaOfStayingUserInMovingPartition ? 1 : 0);

        int deltaReplicas = numReplicasToAdd - numReplicasToDelete;

        return curReplicas + deltaReplicas;
    }

    public static TShortSet findReplicasToAddToTargetPartition(RepUser movingUser, short targetPid, TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> uidToReplicasMap) {
        TShortSet replicasToAddInStayingPartition = new TShortHashSet();
        for(TShortIterator iter = movingUser.getFriendIDs().iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            short pid = uidToPidMap.get(friendId);
            TShortSet replicaPids = uidToReplicasMap.get(friendId);
            if (targetPid != pid && !replicaPids.contains(targetPid)) {
                replicasToAddInStayingPartition.add(friendId);
            }
        }

        return replicasToAddInStayingPartition;
    }

    public static TShortSet findReplicasInMovingPartitionToDelete(RepUser movingUser, TShortSet replicasToBeAdded, int minNumReplicas, TShortObjectMap<TShortSet> uidToReplicasMap, TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> friendships) {
        TShortSet replicasInMovingPartitionToDelete = new TShortHashSet();

        for(TShortIterator iter = findReplicasInPartitionThatWereOnlyThereForThisUsersSake(movingUser, uidToPidMap, friendships).iterator(); iter.hasNext(); ) {
            short replicaId = iter.next();
            int numExistingReplicas = uidToReplicasMap.get(replicaId).size();
            if (numExistingReplicas + (replicasToBeAdded.contains(replicaId) ? 1 : 0) > minNumReplicas) {
                replicasInMovingPartitionToDelete.add(replicaId);
            }
        }

        return replicasInMovingPartitionToDelete;
    }

    public static TShortSet findReplicasInPartitionThatWereOnlyThereForThisUsersSake(RepUser user, TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> friendships) {
        TShortSet replicasThatWereJustThereForThisUsersSake = new TShortHashSet();
        outer: for(TShortIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            if (uidToPidMap.get(friendId) != user.getBasePid()) {
                for(TShortIterator iter2 = friendships.get(friendId).iterator(); iter2.hasNext(); ) {
                    short friendOfFriendId = iter2.next();
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

    static boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(RepUser movingUser, TShortShortMap uidToPidMap) {
        for(TShortIterator iter = movingUser.getFriendIDs().iterator(); iter.hasNext(); ) {
            if (movingUser.getBasePid() == uidToPidMap.get(iter.next())) {
                return true;
            }
        }

        return false;
    }

    static boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition(RepUser movingUser, RepUser stayingUser, int minNumReplicas, TShortShortMap uidToPidMap) {
        boolean couldWeDeleteReplicaOfMovingUserInStayingPartition = movingUser.getReplicaPids().contains(stayingUser.getBasePid());
        int numReplicas = movingUser.getReplicaPids().size();
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(movingUser, uidToPidMap);
        int redundancyOfMovingUser = numReplicas + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        return couldWeDeleteReplicaOfMovingUserInStayingPartition && redundancyOfMovingUser > minNumReplicas;
    }

    static boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser, int minNumReplicas, TShortShortMap uidToPidMap) {
        boolean couldWeDeleteReplicaOfStayingUserInMovingPartition = couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser, uidToPidMap);
        int redundancyOfStayingUser = stayingUser.getReplicaPids().size();
        return couldWeDeleteReplicaOfStayingUserInMovingPartition && redundancyOfStayingUser > minNumReplicas;
    }

    static boolean couldWeDeleteReplicaOfStayingUserInMovingPartition(RepUser movingUser, RepUser stayingUser, TShortShortMap uidToPidMap) {
        boolean movingPartitionHasStayingUserReplica = stayingUser.getReplicaPids().contains(movingUser.getBasePid());
        boolean stayingUserHasNoOtherFriendMastersInMovingPartition = true; //deleting staying user replica is a possibility, if it exists, subject to balance constraints
        for(TShortIterator iter = stayingUser.getFriendIDs().iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            boolean friendIsNotMovingUser = friendId != movingUser.getId();
            boolean friendsMasterIsOnPartition = uidToPidMap.get(friendId) == movingUser.getBasePid();
            if (friendIsNotMovingUser && friendsMasterIsOnPartition) {
                stayingUserHasNoOtherFriendMastersInMovingPartition = false;
            }
        }

        return movingPartitionHasStayingUserReplica && stayingUserHasNoOtherFriendMastersInMovingPartition;
    }
}