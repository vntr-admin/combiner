package io.vntr.middleware;

import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.SBefriender;
import io.vntr.migration.SMigrator;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SparmesRepartitioner;
import io.vntr.manager.RepManager;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.utils.Utils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static java.util.Collections.singleton;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesMiddleware implements IMiddlewareAnalyzer {

    private int minNumReplicas;
    private float gamma;
    private int k;
    private RepManager manager;

    public SparmesMiddleware(int minNumReplicas, float gamma, int k, RepManager manager) {
        this.minNumReplicas = minNumReplicas;
        this.gamma = gamma;
        this.k = k;
        this.manager = manager;
    }

    @Override
    public int addUser() {
        return manager.addUser();
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(Integer userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = manager.getUserMaster(smallerUserId);
        RepUser largerUser = manager.getUserMaster(largerUserId);

        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid  = largerUser.getBasePid();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPids().contains(largerUserPid) && largerUser.getReplicaPids().contains(smallerUserPid);
        if(!colocatedMasters && !colocatedReplicas){
            BEFRIEND_REBALANCE_STRATEGY strategy = SBefriender.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser, manager.getMinNumReplicas(), manager.getFriendships(), manager.getPartitionToUserMap(), manager.getPartitionToReplicasMap());
            performRebalace(strategy, smallerUserId, largerUserId);
        }

        manager.befriend(smallerUser, largerUser);
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
        RepUser smallerUser = manager.getUserMaster(smallUid);
        RepUser largerUser = manager.getUserMaster(largeUid);
        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid = largerUser.getBasePid();

        if (strategy == NO_CHANGE) {
            if (!smallerUser.getReplicaPids().contains(largerUserPid)) {
                manager.addReplica(smallerUser, largerUserPid);
            }
            if (!largerUser.getReplicaPids().contains(smallerUserPid)) {
                manager.addReplica(largerUser, smallerUserPid);
            }
        } else {
            RepUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Integer targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Map<Integer, Integer> uidToPidMap = getUToMasterMap(manager.getPartitionToUserMap());
            Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(manager.getPartitionToReplicasMap(), manager.getUids());

            Set<Integer> replicasToAddInDestinationPartition = SBefriender.findReplicasToAddToTargetPartition(moving, targetPid, uidToPidMap, uidToReplicasMap);
            Set<Integer> replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition, manager.getMinNumReplicas(), uidToReplicasMap, uidToPidMap, manager.getFriendships());
            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
            manager.increaseTally(1);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = manager.getUserMaster(smallerUserId);
        RepUser largerUser = manager.getUserMaster(largerUserId);

        if (!smallerUser.getBasePid().equals(largerUser.getBasePid())) {
            Map<Integer, Integer> uidToPidMap = getUToMasterMap(manager.getPartitionToUserMap());
            Map<Integer, Set<Integer>> friendships = manager.getFriendships();
            boolean smallerReplicaWasOnlyThereForLarger = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser, uidToPidMap, friendships).contains(smallerUserId);
            boolean largerReplicaWasOnlyThereForSmaller = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser, uidToPidMap, friendships).contains(largerUserId);

            if (smallerReplicaWasOnlyThereForLarger && smallerUser.getReplicaPids().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(smallerUser, largerUser.getBasePid());
            }
            if (largerReplicaWasOnlyThereForSmaller && largerUser.getReplicaPids().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(largerUser, smallerUser.getBasePid());
            }
        }

        manager.unfriend(smallerUser, largerUser);
    }

    @Override
    public int addPartition() {
        int pid = manager.addPartition();
        return pid;
    }

    @Override
    public void addPartition(Integer partitionId) {
        manager.addPartition(partitionId);
    }

    @Override
    public void removePartition(Integer partitionId) {
        //First, determine which users will be impacted by this action
        Set<Integer> affectedUsers = determineAffectedUsers(partitionId);

        //Second, determine the migration strategy
        Map<Integer, Integer> migrationStrategy = SMigrator.getUserMigrationStrategy(partitionId, manager.getFriendships(), manager.getPartitionToUserMap(), manager.getPartitionToReplicasMap());

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            RepUser user = manager.getUserMaster(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPartitionId)) {
                if(manager.getMinNumReplicas() > 0) {
                    throw new RuntimeException("This should never happen with minNumReplicas > 0!");
                }
                manager.addReplica(user, newPartitionId);
            }
            manager.promoteReplicaToMaster(userId, newPartitionId);
        }

        Set<Integer> usersToReplicate = getUsersToReplicate(affectedUsers, partitionId);

        //Fourth, add replicas as appropriate
        for (Integer userId : usersToReplicate) {
            RepUser user = manager.getUserMaster(userId);
            manager.addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user, singleton(partitionId)));
        }

        //TODO: ensure this is correct
        //Fifth, remove references to replicas formerly on this partition

        for(Integer uid : manager.getReplicasOnPartition(partitionId)) {
            RepUser user = manager.getUserMaster(uid);
            for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
                manager.getReplicaOnPartition(user.getId(), currentReplicaPartitionId).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        manager.removePartition(partitionId);
    }

    Set<Integer> getUsersToReplicate(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> numReplicasAndMastersNotOnPartitionToBeRemoved = getCountOfReplicasAndMastersNotOnPartition(uids, pid);
        int minReplicas = manager.getMinNumReplicas();
        Set<Integer> usersToReplicate = new HashSet<>();
        for(Integer uid : uids) {
            if(numReplicasAndMastersNotOnPartitionToBeRemoved.get(uid) <= minReplicas) {
                usersToReplicate.add(uid);
            }
        }
        return usersToReplicate;
    }

    Map<Integer, Integer> getCountOfReplicasAndMastersNotOnPartition(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> counts = new HashMap<>();
        for(Integer uid : uids) {
            int count = 0;
            RepUser user = manager.getUserMaster(uid);
            count += user.getBasePid().equals(pid) ? 0 : 1;
            Set<Integer> replicas = user.getReplicaPids();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    Set<Integer> determineAffectedUsers(Integer partitionIdToBeRemoved) {
        Set<Integer> possibilities = new HashSet<>(manager.getMastersOnPartition(partitionIdToBeRemoved));
        possibilities.addAll(manager.getReplicasOnPartition(partitionIdToBeRemoved));
        return possibilities;
    }

    Set<Integer> determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        Set<Integer> possibilities = new HashSet<>(manager.getMastersOnPartition(partitionIdToBeRemoved));
        possibilities.addAll(manager.getReplicasOnPartition(partitionIdToBeRemoved));

        Set<Integer> usersInNeedOfNewReplicas = new HashSet<>();
        for (Integer userId : possibilities) {
            RepUser user = manager.getUserMaster(userId);
            if (user.getReplicaPids().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(RepUser user, Collection<Integer> pidsToExclude) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(manager.getPids());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }


    @Override
    public Integer getNumberOfPartitions() {
        return manager.getPids().size();
    }

    @Override
    public Integer getNumberOfUsers() {
        return manager.getNumUsers();
    }

    @Override
    public Integer getNumberOfFriendships() {
        int numFriendships=0;
        Map<Integer, Set<Integer>> friendships = getFriendships();
        for(Integer uid : friendships.keySet()) {
            numFriendships += friendships.get(uid).size();
        }
        return numFriendships / 2;
    }

    @Override
    public Collection<Integer> getUserIds() {
        return manager.getUids();
    }

    @Override
    public Collection<Integer> getPartitionIds() {
        return manager.getPids();
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Long getMigrationTally() {
        return manager.getMigrationTally();
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        return manager.getPartitionToUserMap();
    }

    @Override
    public Integer getReplicationCount() {
        return manager.getReplicationCount();
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        return manager.getFriendships();
    }

    @Override
    public double calculateAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicaMap() {
        Map<Integer, Set<Integer>> m = new HashMap<>();
        for(int pid : getPartitionIds()) {
            m.put(pid, manager.getReplicasOnPartition(pid));
        }
        return m;
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    @Override
    public double calculateExpectedQueryDelay() {
        return 0; //Replica systems are strictly-local by design.
    }

    @Override
    public void checkValidity() {
        manager.checkValidity();
    }


    public void repartition() {
        RepResults repResults = SparmesRepartitioner.repartition(k, 100, gamma, minNumReplicas, getPartitionToUserMap(), manager.getPartitionToReplicasMap(), getFriendships());
        manager.increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults);
    }

    void physicallyMigrate(RepResults repResults) {
        Map<Integer, Integer> currentUidToPidMap = getUToMasterMap(getPartitionToUserMap());
        Map<Integer, Set<Integer>> currentUidToReplicasMap = getUToReplicasMap(manager.getPartitionToReplicasMap(), manager.getUids());

        for(int uid : manager.getUids()) {
            int currentPid = currentUidToPidMap.get(uid);
            int newPid = repResults.getUidToPidMap().get(uid);

            //move master
            if(currentPid != newPid) {
                manager.moveMasterAndInformReplicas(uid, currentPid, newPid);
                manager.increaseTally(1);
            }

            //add and remove replicas as specified in the repResults
            Set<Integer> newReplicas = repResults.getUidsToReplicaPids().get(uid);
            Set<Integer> currentReplicas = currentUidToReplicasMap.get(uid);
            if(!currentReplicas.equals(newReplicas)) {
                for(int newReplica : newReplicas) {
                    if(!currentReplicas.contains(newReplica)) {
                        manager.addReplica(manager.getUserMaster(uid), newReplica);
                    }
                }
                for(int oldReplica : currentReplicas) {
                    if(!newReplicas.contains(oldReplica)) {
                        manager.removeReplica(manager.getUserMaster(uid), oldReplica);
                    }
                }
            }
        }

        //shore up friend replicas (ideally would be unnecessary)
        for(int uid : manager.getUids()) {
            shoreUpFriendReplicas(uid);
        }

        //ensure k replication
        for(int uid : manager.getUids()) {
            ensureKReplication(uid);
        }
    }

    void shoreUpFriendReplicas(int uid) {
        RepUser user = manager.getUserMaster(uid);
        int pid = user.getBasePid();
        Set<Integer> friends = new HashSet<>(user.getFriendIDs());
        friends.removeAll(manager.getMastersOnPartition(pid));
        friends.removeAll(manager.getReplicasOnPartition(pid));
        for(int friendId : friends) {
            manager.addReplica(manager.getUserMaster(friendId), pid);
        }
    }

    void ensureKReplication(int uid) {
        RepUser user = manager.getUserMaster(uid);
        Set<Integer> replicaLocations = user.getReplicaPids();
        int deficit = minNumReplicas - replicaLocations.size();
        if(deficit > 0) {
            Set<Integer> newLocations = new HashSet<>(getPartitionIds());
            newLocations.removeAll(replicaLocations);
            newLocations.remove(user.getBasePid());
            for(int rPid : ProbabilityUtils.getKDistinctValuesFromList(deficit, newLocations)) {
                manager.addReplica(user, rPid);
            }
        }
    }
}
