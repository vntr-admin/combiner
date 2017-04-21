package io.vntr.spj2;

import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.spj2.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.spj2.BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;

public class SpJ2Middleware implements IMiddlewareAnalyzer {
    private SpJ2Manager manager;
    private SpJ2BefriendingStrategy spJ2BefriendingStrategy;
    private SpJ2MigrationStrategy spJ2MigrationStrategy;

    public SpJ2Middleware(SpJ2Manager manager) {
        this.manager = manager;
        spJ2BefriendingStrategy = new SpJ2BefriendingStrategy(manager);
        spJ2MigrationStrategy = new SpJ2MigrationStrategy(manager);
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
        SpJ2User smallerUser = manager.getUserMasterById(smallerUserId);
        SpJ2User largerUser = manager.getUserMasterById(largerUserId);

        Integer smallerUserPid = smallerUser.getMasterPid();
        Integer largerUserPid  = largerUser.getMasterPid();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPids().contains(largerUserPid) && largerUser.getReplicaPids().contains(smallerUserPid);
        if (!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = spJ2BefriendingStrategy.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser);
            performRebalace(strategy, smallerUserId, largerUserId);
        }

        manager.befriend(smallerUser, largerUser);
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
        SpJ2User smallerUser = manager.getUserMasterById(smallUid);
        SpJ2User largerUser = manager.getUserMasterById(largeUid);
        Integer smallerUserPid = smallerUser.getMasterPid();
        Integer largerUserPid = largerUser.getMasterPid();

        Object k = NO_CHANGE;
        if (strategy == NO_CHANGE) {
            if (!smallerUser.getReplicaPids().contains(largerUserPid)) {
                manager.addReplica(smallerUser, largerUserPid);
            }
            if (!largerUser.getReplicaPids().contains(smallerUserPid)) {
                manager.addReplica(largerUser, smallerUserPid);
            }
        } else {
            SpJ2User moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Integer targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Set<Integer> replicasToAddInDestinationPartition = spJ2BefriendingStrategy.findReplicasToAddToTargetPartition(moving, targetPid);
            Set<Integer> replicasToDeleteInSourcePartition = spJ2BefriendingStrategy.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition);
            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        //When an edge between smallerUserId and largerUserId disappears,
        //the algorithm removes the replica of smallerUserId in the partition holding the master of node largerUserId
        //if no other node requires it, and vice-versa.
        //The algorithm checks whether there are more than K slave replicas before removing the node so that the desired redundancy level is maintained.

        SpJ2User smallerUser = manager.getUserMasterById(smallerUserId);
        SpJ2User largerUser = manager.getUserMasterById(largerUserId);

        if (!smallerUser.getMasterPid().equals(largerUser.getMasterPid())) {
            boolean smallerReplicaWasOnlyThereForLarger = spJ2BefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser).contains(smallerUserId);
            boolean largerReplicaWasOnlyThereForSmaller = spJ2BefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser).contains(largerUserId);

            if (smallerReplicaWasOnlyThereForLarger && smallerUser.getReplicaPids().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(smallerUser, largerUser.getMasterPid());
            }
            if (largerReplicaWasOnlyThereForSmaller && largerUser.getReplicaPids().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(largerUser, smallerUser.getMasterPid());
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
        Map<Integer, Integer> migrationStrategy = spJ2MigrationStrategy.getUserMigrationStrategy(partitionId);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            SpJ2User user = manager.getUserMasterById(userId);
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
            SpJ2User user = manager.getUserMasterById(userId);
            int newPid = getRandomPartitionIdWhereThisUserIsNotPresent(user, Arrays.asList(partitionId));
            manager.addReplica(user, newPid);
        }

        //Fifth, remove references to replicas formerly on this partition
        for(Integer uid : manager.getPartitionById(partitionId).getIdsOfReplicas()) {
            SpJ2User user = manager.getUserMasterById(uid);
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
            SpJ2User user = manager.getUserMasterById(userId);
            if (user.getReplicaPids().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for (Integer userId : partition.getIdsOfReplicas()) {
            SpJ2User user = manager.getUserMasterById(userId);
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
            SpJ2User user = manager.getUserMasterById(uid);
            count += user.getMasterPid().equals(pid) ? 0 : 1;
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

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SpJ2User user, Collection<Integer> pidsToExclude) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(manager.getAllPartitionIds());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getMasterPid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    @Override
    public Integer getNumberOfPartitions() {
        return manager.getAllPartitionIds().size();
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
        return manager.getAllUserIds();
    }

    @Override
    public Collection<Integer> getPartitionIds() {
        return manager.getAllPartitionIds();
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
