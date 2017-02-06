package io.vntr.sparmes;

import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.sparmes.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesMiddleware implements IMiddlewareAnalyzer {

    SparmesManager manager;
    private SparmesBefriendingStrategy sparmesBefriendingStrategy;
    private SparmesMigrationStrategy sparmesMigrationStrategy;

    public SparmesMiddleware(SparmesManager manager, SparmesBefriendingStrategy sparmesBefriendingStrategy, SparmesMigrationStrategy sparmesMigrationStrategy) {
        this.manager = manager;
        this.sparmesBefriendingStrategy = sparmesBefriendingStrategy;
        this.sparmesMigrationStrategy = sparmesMigrationStrategy;
    }

    public SparmesMiddleware(SparmesManager manager) {
        this.manager = manager;
        sparmesBefriendingStrategy = new SparmesBefriendingStrategy(manager);
        sparmesMigrationStrategy = new SparmesMigrationStrategy(manager);
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
        SparmesUser smallerUser = manager.getUserMasterById(smallerUserId);
        SparmesUser largerUser = manager.getUserMasterById(largerUserId);

        Integer smallerUserPid = smallerUser.getMasterPartitionId();
        Integer largerUserPid  = largerUser.getMasterPartitionId();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPartitionIds().contains(largerUserPid) && largerUser.getReplicaPartitionIds().contains(smallerUserPid);
        if(!colocatedMasters && !colocatedReplicas){
            BEFRIEND_REBALANCE_STRATEGY strategy = sparmesBefriendingStrategy.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser);
            performRebalace(strategy, smallerUserId, largerUserId);
        }

        manager.befriend(smallerUser, largerUser);
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
        SparmesUser smallerUser = manager.getUserMasterById(smallUid);
        SparmesUser largerUser = manager.getUserMasterById(largeUid);
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
            SparmesUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Integer targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Set<Integer> replicasToAddInDestinationPartition = sparmesBefriendingStrategy.findReplicasToAddToTargetPartition(moving, targetPid);
            Set<Integer> replicasToDeleteInSourcePartition = sparmesBefriendingStrategy.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition);
            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        SparmesUser smallerUser = manager.getUserMasterById(smallerUserId);
        SparmesUser largerUser = manager.getUserMasterById(largerUserId);

        if (!smallerUser.getMasterPartitionId().equals(largerUser.getMasterPartitionId())) {
            boolean smallerReplicaWasOnlyThereForLarger = sparmesBefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser).contains(smallerUserId);
            boolean largerReplicaWasOnlyThereForSmaller = sparmesBefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser).contains(largerUserId);

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
    public int addPartition() {
        int pid = manager.addPartition();
//        manager.repartition();
        return pid;
    }

    @Override
    public void addPartition(Integer partitionId) {
        manager.addPartition(partitionId);
//        manager.repartition();
    }

    @Override
    public void removePartition(Integer partitionId) {
        //First, determine which users will be impacted by this action
        Set<Integer> affectedUsers = determineAffectedUsers(partitionId);
//        //First, determine which users will need more replicas once this partition is kaput
//        Set<Integer> usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(partitionId);

        //Second, determine the migration strategy
        Map<Integer, Integer> migrationStrategy = sparmesMigrationStrategy.getUserMigrationStrategy(partitionId);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            SparmesUser user = manager.getUserMasterById(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPartitionIds().contains(newPartitionId)) {
                manager.addReplica(user, newPartitionId);
//                usersInNeedOfNewReplicas.remove(userId);
            }
            manager.promoteReplicaToMaster(userId, newPartitionId);
        }

        Set<Integer> usersToReplicate = getUsersToReplicate(affectedUsers, partitionId);

        //Fourth, add replicas as appropriate
        for (Integer userId : usersToReplicate) {
            SparmesUser user = manager.getUserMasterById(userId);
            manager.addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user, Arrays.asList(partitionId)));
        }

        //TODO: ensure this is correct
        //Fifth, remove references to replicas formerly on this partition
        for(Integer uid : manager.getPartitionById(partitionId).getIdsOfReplicas()) {
            SparmesUser user = manager.getUserMasterById(uid);
            for (Integer currentReplicaPartitionId : user.getReplicaPartitionIds()) {
                manager.getPartitionById(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        manager.removePartition(partitionId);

//        manager.repartition();
    }

    Set<Integer> getUsersToReplicate(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> numReplicasAndMastersNotOnPartitionToBeRemoved = getCountOfReplicasAndMastersNotOnPartition(uids, pid);
        int minReplicas = manager.getMinNumReplicas();
        Set<Integer> usersToReplicate = new HashSet<Integer>();
        for(Integer uid : uids) {
            if(numReplicasAndMastersNotOnPartitionToBeRemoved.get(uid) <= minReplicas) {
                usersToReplicate.add(uid);
            }
        }
        return usersToReplicate;
    }

    Map<Integer, Integer> getCountOfReplicasAndMastersNotOnPartition(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
        for(Integer uid : uids) {
            int count = 0;
            SparmesUser user = manager.getUserMasterById(uid);
            count += user.getMasterPartitionId().equals(pid) ? 0 : 1;
            Set<Integer> replicas = user.getReplicaPartitionIds();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    Set<Integer> determineAffectedUsers(Integer partitionIdToBeRemoved) {
        SparmesPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Integer> possibilities = new HashSet<Integer>(partition.getIdsOfMasters());
        possibilities.addAll(partition.getIdsOfReplicas());
        return possibilities;
    }

    Set<Integer> determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        SparmesPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Integer> possibilities = new HashSet<Integer>(partition.getIdsOfMasters());
        possibilities.addAll(partition.getIdsOfReplicas());

        Set<Integer> usersInNeedOfNewReplicas = new HashSet<Integer>();
        for (Integer userId : possibilities) {
            SparmesUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPartitionIds().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SparmesUser user, Collection<Integer> pidsToExclude) {
        Set<Integer> potentialReplicaLocations = new HashSet<Integer>(manager.getAllPartitionIds());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
        List<Integer> list = new LinkedList<Integer>(potentialReplicaLocations);
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
    public double calcualteAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicaMap() {
        Map<Integer, Set<Integer>> m = new HashMap<Integer, Set<Integer>>();
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
}
