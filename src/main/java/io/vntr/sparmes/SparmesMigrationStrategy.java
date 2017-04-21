package io.vntr.sparmes;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesMigrationStrategy {
    private SparmesManager manager;

    public SparmesMigrationStrategy(SparmesManager manager) {
        this.manager = manager;
    }

    public Map<Integer, Integer> getUserMigrationStrategy(Integer partitionId) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        Set<Integer> partitionIdsToSkip = new HashSet<>(Arrays.asList(partitionId));
        final SparmesPartition partition = manager.getPartitionById(partitionId);
        Set<Integer> masterIds = partition.getIdsOfMasters();

        NavigableSet<Score> scores = new TreeSet<>();
        for (Integer userId : masterIds) {
            SparmesUser user = manager.getUserMasterById(userId);
            for (Integer replicaPartitionId : user.getReplicaPids()) {
                scores.add(new Score(userId, replicaPartitionId, scoreReplicaPromotion(user, replicaPartitionId)));
            }
        }

        Map<Integer, Integer> remainingSpotsInPartitions = getRemainingSpotsInPartitions(partitionIdsToSkip);

        Map<Integer, Integer> strategy = new HashMap<>();

        for (Iterator<Score> iter = scores.descendingIterator(); iter.hasNext(); ) {
            Score score = iter.next();
            int remainingSpotsInPartition = remainingSpotsInPartitions.get(score.partitionId);
            if (!strategy.containsKey(score.userId) && remainingSpotsInPartition > 0 && score.score > 0) {
                strategy.put(score.userId, score.partitionId);
                remainingSpotsInPartitions.put(score.partitionId, remainingSpotsInPartition - 1);
            }
        }

        Set<Integer> usersYetUnplaced = new HashSet<>(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        for (Integer uid : usersYetUnplaced) {
            Integer leastOverloadedPid = getLeastOverloadedPartitionWhereThisUserHasAReplica(uid, strategy, manager.getAllPartitionIds());
            if(leastOverloadedPid != null) {
                strategy.put(uid, leastOverloadedPid);
            }
            else {
                strategy.put(uid, getLeastOverloadedPartition(manager.getAllPartitionIds(), strategy, partitionId));
            }
        }

        return strategy;
    }

    Integer getLeastOverloadedPartition(Set<Integer> allPids, Map<Integer, Integer> strategy, Integer pidToDelete) {
        Map<Integer, Integer> pToStrategyCount = new HashMap<>();
        for(Integer pid : allPids) {
            pToStrategyCount.put(pid, 0);
        }
        for(Integer uid1 : strategy.keySet()) {
            int pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid));
        }

        int minMasters = Integer.MAX_VALUE;
        Integer minPid = null;
        for(Integer pid : allPids) {
            if(pid.equals(pidToDelete)) {
                continue;
            }
            int numMasters = manager.getPartitionById(pid).getNumMasters() + pToStrategyCount.get(pid);
            if(numMasters < minMasters) {
                minMasters = numMasters;
                minPid = pid;
            }
        }
        return minPid;
    }

    Integer getLeastOverloadedPartitionWhereThisUserHasAReplica(Integer uid, Map<Integer, Integer> strategy, Set<Integer> allPids) {
        Map<Integer, Integer> pToStrategyCount = new HashMap<>();
        for(Integer pid : allPids) {
            pToStrategyCount.put(pid, 0);
        }
        for(Integer uid1 : strategy.keySet()) {
            Integer pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid));
        }

        SparmesUser user = manager.getUserMasterById(uid);
        int minMasters = Integer.MAX_VALUE;
        Integer minPid = null;
        for(Integer pid : user.getReplicaPids()) {
            int numMasters = manager.getPartitionById(pid).getNumMasters() + pToStrategyCount.get(pid);
            if(numMasters < minMasters) {
                minMasters = numMasters;
                minPid = pid;
            }
        }
        return minPid;
    }

    float scoreReplicaPromotion(SparmesUser user, Integer replicaPartitionId) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = 0;
        for (Integer friendId : user.getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (friend.getMasterPid().equals(replicaPartitionId)) {
                numFriendsOnPartition++;
            }
        }

        int numFriendsTotal = user.getFriendIDs().size();

        if(numFriendsTotal == 0) {
            return Float.MIN_VALUE;
        }

        return ((float) (numFriendsOnPartition * numFriendsOnPartition)) / numFriendsTotal;
    }

    Map<Integer, Integer> getRemainingSpotsInPartitions(Set<Integer> partitionIdsToSkip) {
        int numUsers = manager.getNumUsers();
        int numPartitions = manager.getAllPartitionIds().size() - partitionIdsToSkip.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        Map<Integer, Integer> partitionToNumMastersMap = new HashMap<>();
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (partitionIdsToSkip.contains(partitionId)) {
                continue;
            }
            SparmesPartition partition = manager.getPartitionById(partitionId);
            partitionToNumMastersMap.put(partition.getId(), partition.getNumMasters());
        }

        Map<Integer, Integer> remainingSpotsInPartitions = new HashMap<>();
        for (Integer partitionId : partitionToNumMastersMap.keySet()) {
            remainingSpotsInPartitions.put(partitionId, maxUsersPerPartition - partitionToNumMastersMap.get(partitionId));
        }

        return remainingSpotsInPartitions;
    }

}
