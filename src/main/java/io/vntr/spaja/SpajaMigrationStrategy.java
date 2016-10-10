package io.vntr.spaja;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaMigrationStrategy {
    private SpajaManager manager;

    public SpajaMigrationStrategy(SpajaManager manager) {
        this.manager = manager;
    }

    public Map<Integer, Integer> getUserMigrationStrategy(Integer partitionId) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        Set<Integer> partitionIdsToSkip = new HashSet<Integer>(Arrays.asList(partitionId));
        final SpajaPartition partition = manager.getPartitionById(partitionId);
        Set<Integer> masterIds = partition.getIdsOfMasters();

        NavigableSet<Score> scores = new TreeSet<Score>();
        for (Integer userId : masterIds) {
            SpajaUser user = manager.getUserMasterById(userId);
            for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
                if(replicaPartitionId.intValue() == partitionId) {
                    continue;
                }
                scores.add(new Score(userId, replicaPartitionId, scoreReplicaPromotion(user, replicaPartitionId)));
            }
        }

        Map<Integer, Integer> remainingSpotsInPartitions = getRemainingSpotsInPartitions(partitionIdsToSkip);
        if(remainingSpotsInPartitions == null) {
            System.out.println("Huh?");
        }

        Map<Integer, Integer> strategy = new HashMap<Integer, Integer>();

        for (Iterator<Score> iter = scores.descendingIterator(); iter.hasNext(); ) {
            Score score = iter.next();
            Integer remainingSpotsInPartition = remainingSpotsInPartitions.get(score.partitionId);
            if(remainingSpotsInPartition == null) {
                System.out.println("Buh?");
            }
            if (!strategy.containsKey(score.userId) && remainingSpotsInPartition > 0 && score.score > 0) {
                strategy.put(score.userId, score.partitionId);
                remainingSpotsInPartitions.put(score.partitionId, remainingSpotsInPartition - 1);
            }
        }

        Set<Integer> usersYetUnplaced = new HashSet<Integer>(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        for (Integer uid : usersYetUnplaced) {
            Set<Integer> acceptablePids = new HashSet<Integer>(manager.getAllPartitionIds());
            acceptablePids.removeAll(partitionIdsToSkip);
            strategy.put(uid, getLeastOverloadedPartitionWhereThisUserHasAReplica(uid, strategy, acceptablePids));
        }

        return strategy;
    }

    Integer getLeastOverloadedPartitionWhereThisUserHasAReplica(Integer uid, Map<Integer, Integer> strategy, Set<Integer> allPids) {
        Map<Integer, Integer> pToStrategyCount = new HashMap<Integer, Integer>();
        for(Integer pid : allPids) {
            pToStrategyCount.put(pid, 0);
        }
        for(Integer uid1 : strategy.keySet()) {
            int pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid));
        }

        SpajaUser user = manager.getUserMasterById(uid);
        int minMasters = Integer.MAX_VALUE;
        Integer minPid = null;
        for(Integer pid : user.getReplicaPartitionIds()) {
            if(!allPids.contains(pid)) {
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

    float scoreReplicaPromotion(SpajaUser user, Integer replicaPartitionId) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = 0;
        for (Integer friendId : user.getFriendIDs()) {
            SpajaUser friend = manager.getUserMasterById(friendId);
            if (friend.getMasterPartitionId().equals(replicaPartitionId)) {
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
        Set<Integer> possibilities = new HashSet<Integer>(manager.getAllPartitionIds());
        possibilities.removeAll(partitionIdsToSkip);
        int numUsers = manager.getNumUsers();
        int numPartitions = possibilities.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        Map<Integer, Integer> partitionToNumMastersMap = new HashMap<Integer, Integer>();
        for (Integer partitionId : possibilities) {
            SpajaPartition partition = manager.getPartitionById(partitionId);
            partitionToNumMastersMap.put(partition.getId(), partition.getNumMasters());
        }

        Map<Integer, Integer> remainingSpotsInPartitions = new HashMap<Integer, Integer>();
        for (Integer partitionId : partitionToNumMastersMap.keySet()) {
            remainingSpotsInPartitions.put(partitionId, maxUsersPerPartition - partitionToNumMastersMap.get(partitionId));
        }

        return remainingSpotsInPartitions;
    }

}
