package io.vntr.replicadummy;

import io.vntr.IMiddlewareAnalyzer;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.migration.DummyMigrator;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyMiddleware implements IMiddlewareAnalyzer {
    private ReplicaDummyManager manager;

    public ReplicaDummyMiddleware(ReplicaDummyManager manager) {
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
        RepUser smallerUser = manager.getUserMasterById(smallerUserId);
        RepUser largerUser = manager.getUserMasterById(largerUserId);
        manager.befriend(smallerUser, largerUser);
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = manager.getUserMasterById(smallerUserId);
        RepUser largerUser = manager.getUserMasterById(largerUserId);
        manager.unfriend(smallerUser, largerUser);
    }

    @Override
    public int addPartition() {
        return manager.addPartition();
    }

    @Override
    public void addPartition(Integer partitionId) {
        manager.addPartition(partitionId);
    }


    @Override
    public void removePartition(Integer partitionId) {
        //First, determine which users will need more replicas once this partition is kaput
        Set<Integer> usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(partitionId);
        
        //Second, determine the migration strategy
        Map<Integer, Integer> migrationStrategy = DummyMigrator.getUserMigrationStrategy(partitionId, manager.getPartitionToUserMap(), manager.getPartitionToReplicasMap());

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            RepUser user = manager.getUserMasterById(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPartitionId)) {
                manager.addReplica(user, newPartitionId);
                usersInNeedOfNewReplicas.remove(userId);
            }
            manager.promoteReplicaToMaster(userId, migrationStrategy.get(userId));
        }

        //Fourth, add replicas as appropriate
        for (Integer userId : usersInNeedOfNewReplicas) {
            RepUser user = manager.getUserMasterById(userId);
            int newPid = getRandomPartitionIdWhereThisUserIsNotPresent(user, Collections.singletonList(partitionId));
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
        ReplicaDummyPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
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
        return 0L; //Replica Dummy doesn't migrate users
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
        //ignores downtime
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
        return 0; //replica systems are strictly-local by design
    }

    @Override
    public void checkValidity() {
        manager.checkValidity();
    }
}
