package io.vntr.spaja;

import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.spaja.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaMiddleware implements IMiddlewareAnalyzer {

    SpajaManager manager;
    private SpajaBefriendingStrategy spajaBefriendingStrategy;
    private SpajaMigrationStrategy spajaMigrationStrategy;
    private SpajaRepartitioner spajaRepartitioner;

    public SpajaMiddleware(SpajaManager manager, SpajaBefriendingStrategy spajaBefriendingStrategy, SpajaMigrationStrategy spajaMigrationStrategy) {
        this.manager = manager;
        this.spajaBefriendingStrategy = spajaBefriendingStrategy;
        this.spajaMigrationStrategy = spajaMigrationStrategy;
    }

    public SpajaMiddleware(SpajaManager manager) {
        this.manager = manager;
        spajaBefriendingStrategy = new SpajaBefriendingStrategy(manager);
        spajaMigrationStrategy = new SpajaMigrationStrategy(manager);
        spajaRepartitioner = new SpajaRepartitioner(manager, spajaBefriendingStrategy);
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
        SpajaUser smallerUser = manager.getUserMasterById(smallerUserId);
        SpajaUser largerUser = manager.getUserMasterById(largerUserId);

        //TODO: on SparMiddleware, we moved the following to the end of the method, so perhaps that's necessary

        Integer smallerUserPid = smallerUser.getMasterPartitionId();
        Integer largerUserPid  = largerUser.getMasterPartitionId();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPartitionIds().contains(largerUserPid) && largerUser.getReplicaPartitionIds().contains(smallerUserPid);
        if (!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = spajaBefriendingStrategy.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser);
            performRebalace(strategy, smallerUserId, largerUserId);
        }

        manager.befriend(smallerUser, largerUser);
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
        SpajaUser smallerUser = manager.getUserMasterById(smallUid);
        SpajaUser largerUser = manager.getUserMasterById(largeUid);
        Integer smallerUserPid = smallerUser.getMasterPartitionId();
        Integer largerUserPid = largerUser.getMasterPartitionId();

        if (strategy == NO_CHANGE) {
            if (!smallerUser.getReplicaPartitionIds().contains(largerUserPid)) {
                manager.addReplica(smallerUser, largerUserPid);
            }
            if (!largerUser.getReplicaPartitionIds().contains(smallerUserPid)) {
                manager.addReplica(largerUser, smallerUserPid);
            }
        } else {
            SpajaUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Integer targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Set<Integer> replicasToAddInDestinationPartition = spajaBefriendingStrategy.findReplicasToAddToTargetPartition(moving, targetPid);
            Set<Integer> replicasToDeleteInSourcePartition = spajaBefriendingStrategy.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition);

            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        SpajaUser smallerUser = manager.getUserMasterById(smallerUserId);
        SpajaUser largerUser = manager.getUserMasterById(largerUserId);

        if (!smallerUser.getMasterPartitionId().equals(largerUser.getMasterPartitionId())) {
            boolean smallerReplicaWasOnlyThereForLarger = spajaBefriendingStrategy.isFriendOnlyThereForThisUsersSake(largerUser, smallerUser);
            boolean largerReplicaWasOnlyThereForSmaller = spajaBefriendingStrategy.isFriendOnlyThereForThisUsersSake(smallerUser, largerUser);

            if (smallerReplicaWasOnlyThereForLarger && smallerUser.getReplicaPartitionIds().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(smallerUser, largerUser.getMasterPartitionId());
            }
            if (largerReplicaWasOnlyThereForSmaller && largerUser.getReplicaPartitionIds().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(largerUser, smallerUser.getMasterPartitionId());
            }
        }

        manager.unfriend(smallerUser, largerUser);
    }

    @Override
    public int addPartition() { return manager.addPartition(); }

    @Override
    public void addPartition(Integer partitionId) {
        manager.addPartition(partitionId);
    }

    @Override
    public void removePartition(Integer partitionId) {
        //First, determine which users will be impacted by this action
        Set<Integer> affectedUsers = determineAffectedUsers(partitionId);

        //Second, determine the migration strategy
        Map<Integer, Integer> migrationStrategy = spajaMigrationStrategy.getUserMigrationStrategy(partitionId);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            SpajaUser user = manager.getUserMasterById(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPartitionIds().contains(newPartitionId)) {
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
            SpajaUser user = manager.getUserMasterById(userId);
            Integer newPid = getRandomPartitionIdWhereThisUserIsNotPresent(user, Arrays.asList(partitionId));
            manager.addReplica(user, newPid);
        }

        //TODO: the following is copied from SparMiddleware; should it be modified?
        // "Fifth, remove references to replicas formerly on this partition"
        for(Integer uid : manager.getPartitionById(partitionId).getIdsOfReplicas()) {
            SpajaUser user = manager.getUserMasterById(uid);
            for (Integer currentReplicaPartitionId : user.getReplicaPartitionIds()) {
                manager.getPartitionById(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(partitionId);
            }
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
            SpajaUser user = manager.getUserMasterById(uid);
            count += user.getMasterPartitionId().equals(pid) ? 0 : 1;
            Set<Integer> replicas = user.getReplicaPartitionIds();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    Set<Integer> determineAffectedUsers(Integer partitionIdToBeRemoved) {
        SpajaPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Integer> possibilities = new HashSet<>(partition.getIdsOfMasters());
        possibilities.addAll(partition.getIdsOfReplicas());
        return possibilities;
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SpajaUser user, Collection<Integer> pidsToExclude) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(manager.getAllPartitionIds());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
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
        spajaRepartitioner.repartition();
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        return manager.getFriendships();
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
        return 0; //Replica systems are strictly-local by design.
    }

    @Override
    public void checkValidity() {
        manager.checkValidity();
    }
}
