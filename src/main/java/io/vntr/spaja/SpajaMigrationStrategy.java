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

    public Map<Long, Long> getUserMigrationStrategy(Long partitionId) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        Set<Long> partitionIdsToSkip = new HashSet<Long>(Arrays.asList(partitionId));
        final SpajaPartition partition = manager.getPartitionById(partitionId);
        Set<Long> masterIds = partition.getIdsOfMasters();

        class Score implements Comparable<Score> {
            public final long userId;
            public final long partitionId;
            public final double score;

            public Score(Long userId, Long partitionId, double score) {
                this.userId = userId;
                this.partitionId = partitionId;
                this.score = score;
            }

            public int compareTo(Score o) {
                if (this.score > o.score) {
                    return 1;
                } else if (this.score < o.score) {
                    return -1;
                } else {
                    if (this.partitionId > o.partitionId) {
                        return 1;
                    } else if (this.partitionId > o.partitionId) {
                        return -1;
                    } else {
                        if (this.userId > o.userId) {
                            return 1;
                        } else if (this.userId < o.userId) {
                            return -1;
                        }

                        return 0;
                    }
                }
            }

            @Override
            public boolean equals(Object o) {
                if (o instanceof Score) {
                    Score s = (Score) o;
                    return s.userId == this.userId && s.partitionId == this.partitionId && s.score == this.score;
                }
                return false;
            }

            @Override
            public int hashCode() {
                return ((int) userId) << 15 + ((int) partitionId);
            }

            @Override
            public String toString() {
//                String scoreStr = String.format("%d: --(%.2f)--> %d", uid, score, newPid);
                return String.format("%3d: --(%.2f)--> %3d", userId, score, partitionId);//uid + ": --(" + score + ")-->" + newPid;
            }
        }

        NavigableSet<Score> scores = new TreeSet<Score>();
        for (Long userId : masterIds) {
            SpajaUser user = manager.getUserMasterById(userId);
            for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
                scores.add(new Score(userId, replicaPartitionId, scoreReplicaPromotion(user, replicaPartitionId)));
            }
        }

        Map<Long, Integer> remainingSpotsInPartitions = getRemainingSpotsInPartitions(partitionIdsToSkip);

        Map<Long, Long> strategy = new HashMap<Long, Long>();

        for (Iterator<Score> iter = scores.descendingIterator(); iter.hasNext(); ) {
            Score score = iter.next();
            int remainingSpotsInPartition = remainingSpotsInPartitions.get(score.partitionId);
            if (!strategy.containsKey(score.userId) && remainingSpotsInPartition > 0) {
                strategy.put(score.userId, score.partitionId);
                remainingSpotsInPartitions.put(score.partitionId, remainingSpotsInPartition - 1);
            }
        }

        Set<Long> usersYetUnplaced = new HashSet<Long>(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        for (Long uid : usersYetUnplaced) {
            strategy.put(uid, getLeastOverloadedPartitionWhereThisUserHasAReplica(uid, strategy, manager.getAllPartitionIds()));
        }

        return strategy;
    }

    Long getLeastOverloadedPartitionWhereThisUserHasAReplica(Long uid, Map<Long, Long> strategy, Set<Long> allPids) {
        Map<Long, Integer> pToStrategyCount = new HashMap<Long, Integer>();
        for(Long pid : allPids) {
            pToStrategyCount.put(pid, 0);
        }
        for(Long uid1 : strategy.keySet()) {
            long pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid));
        }

        SpajaUser user = manager.getUserMasterById(uid);
        int minMasters = Integer.MAX_VALUE;
        Long minPid = null;
        for(Long pid : user.getReplicaPartitionIds()) {
            int numMasters = manager.getPartitionById(pid).getNumMasters() + pToStrategyCount.get(pid);
            if(numMasters < minMasters) {
                minMasters = numMasters;
                minPid = pid;
            }
        }
        return minPid;
    }

    double scoreReplicaPromotion(SpajaUser user, Long replicaPartitionId) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = 0;
        for (Long friendId : user.getFriendIDs()) {
            SpajaUser friend = manager.getUserMasterById(friendId);
            if (friend.getMasterPartitionId().equals(replicaPartitionId)) {
                numFriendsOnPartition++;
            }
        }

        int numFriendsTotal = user.getFriendIDs().size();

        return ((double) (numFriendsOnPartition * numFriendsOnPartition)) / (numFriendsTotal);
    }

    Map<Long, Integer> getRemainingSpotsInPartitions(Set<Long> partitionIdsToSkip) {
        int numUsers = manager.getNumUsers();
        int numPartitions = manager.getAllPartitionIds().size() - partitionIdsToSkip.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        Map<Long, Integer> partitionToNumMastersMap = new HashMap<Long, Integer>();
        for (Long partitionId : manager.getAllPartitionIds()) {
            if (partitionIdsToSkip.contains(partitionId)) {
                continue;
            }
            SpajaPartition partition = manager.getPartitionById(partitionId);
            partitionToNumMastersMap.put(partition.getId(), partition.getNumMasters());
        }

        Map<Long, Integer> remainingSpotsInPartitions = new HashMap<Long, Integer>();
        for (Long partitionId : partitionToNumMastersMap.keySet()) {
            remainingSpotsInPartitions.put(partitionId, maxUsersPerPartition - partitionToNumMastersMap.get(partitionId));
        }

        return remainingSpotsInPartitions;
    }

}
