package io.vntr.spj2;

import io.vntr.IMiddlewareAnalyzer;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.Utils;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.SBefriender;
import io.vntr.migration.SMigrator;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

public class SpJ2Middleware implements IMiddlewareAnalyzer {
    private SpJ2Manager manager;
//    private SpJ2BefriendingStrategy spJ2BefriendingStrategy;

    public SpJ2Middleware(SpJ2Manager manager) {
        this.manager = manager;
//        spJ2BefriendingStrategy = new SpJ2BefriendingStrategy(manager);
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
        RepUser smallerUser = manager.getUserMasterById(smallerUserId);
        RepUser largerUser = manager.getUserMasterById(largerUserId);

        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid  = largerUser.getBasePid();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPids().contains(largerUserPid) && largerUser.getReplicaPids().contains(smallerUserPid);
        if (!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = SBefriender.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser, manager.getMinNumReplicas(), manager.getFriendships(), manager.getPartitionToUserMap(), manager.getPartitionToReplicasMap());
            performRebalace(strategy, smallerUserId, largerUserId);
        }

        manager.befriend(smallerUser, largerUser);
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
        RepUser smallerUser = manager.getUserMasterById(smallUid);
        RepUser largerUser = manager.getUserMasterById(largeUid);
        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid = largerUser.getBasePid();

        Object k = NO_CHANGE;
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
            Map<Integer, Integer> uidToPidMap = Utils.getUToMasterMap(manager.getPartitionToUserMap());
            Map<Integer, Set<Integer>> uidToReplicasMap = Utils.getUToReplicasMap(manager.getPartitionToReplicasMap(), manager.getUids());

            Set<Integer> replicasToAddInDestinationPartition = SBefriender.findReplicasToAddToTargetPartition(moving, targetPid, uidToPidMap, uidToReplicasMap);
            Set<Integer> replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition, manager.getMinNumReplicas(), uidToReplicasMap, uidToPidMap, manager.getFriendships());
            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        //When an edge between smallerUserId and largerUserId disappears,
        //the algorithm removes the replica of smallerUserId in the partition holding the master of node largerUserId
        //if no other node requires it, and vice-versa.
        //The algorithm checks whether there are more than K slave replicas before removing the node so that the desired redundancy level is maintained.

        RepUser smallerUser = manager.getUserMasterById(smallerUserId);
        RepUser largerUser = manager.getUserMasterById(largerUserId);

        if (!smallerUser.getBasePid().equals(largerUser.getBasePid())) {
            Map<Integer, Integer> uidToPidMap = Utils.getUToMasterMap(manager.getPartitionToUserMap());
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
        //We use option (2) from the paper:
        //2) let the re-distribution of the masters be the result of the node and edge arrival processes and the load-balancing condition.
        return manager.addPartition();
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
            RepUser user = manager.getUserMasterById(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPartitionId)) {
                if(manager.getMinNumReplicas() > 0) {
                    throw new RuntimeException("This should never happen with minNumReplicas > 0!");
                }
                manager.addReplica(user, newPartitionId);
            }
            manager.promoteReplicaToMaster(userId, migrationStrategy.get(userId));
        }

        Set<Integer> usersToReplicate = getUsersToReplicate(affectedUsers, partitionId);

        //Fourth, add replicas as appropriate
        for (Integer userId : usersToReplicate) {
            RepUser user = manager.getUserMasterById(userId);
            int newPid = getRandomPartitionIdWhereThisUserIsNotPresent(user, Collections.singleton(partitionId));
            manager.addReplica(user, newPid);
        }

        //Fifth, remove references to replicas formerly on this partition
        for(Integer uid : manager.getPartitionById(partitionId).getIdsOfReplicas()) {
            RepUser user = manager.getUserMasterById(uid);
            for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
                manager.getPartitionById(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        manager.removePartition(partitionId);
    }

    Set<Integer> determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        SpJ2Partition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Integer> usersInNeedOfNewReplicas = new HashSet<>();

        //First, determine which users will need more replicas once this partition is kaput
        for (Integer userId : partition.getIdsOfMasters()) {
            RepUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPids().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for (Integer userId : partition.getIdsOfReplicas()) {
            RepUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPids().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
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
            RepUser user = manager.getUserMasterById(uid);
            count += user.getBasePid().equals(pid) ? 0 : 1;
            Set<Integer> replicas = user.getReplicaPids();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    Set<Integer> determineAffectedUsers(Integer partitionIdToBeRemoved) {
        SpJ2Partition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Integer> possibilities = new HashSet<>(partition.getIdsOfMasters());
        possibilities.addAll(partition.getIdsOfReplicas());
        return possibilities;
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
        return numFriendships / 2; //TODO: make sure this is correct
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
        manager.repartition();
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
            m.put(pid, manager.getPartitionById(pid).getIdsOfReplicas());
        }
        return m;
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    @Override
    public double calculateExpectedQueryDelay() {
        return 0; //Replica systems are strictly-local by design
    }

    @Override
    public void checkValidity() {
        manager.checkValidity();
    }
}
