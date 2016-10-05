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

        class Score implements Comparable<Score> {
            public final int userId;
            public final int partitionId;
            public final float score;

            public Score(Integer userId, Integer partitionId, float score) {
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
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Score score1 = (Score) o;

                if (userId != score1.userId) return false;
                if (partitionId != score1.partitionId) return false;
                return Float.compare(score1.score, score) == 0;

            }

            @Override
            public int hashCode() {
                int result = userId;
                result = 31 * result + partitionId;
                result = 31 * result + (score != +0.0f ? Float.floatToIntBits(score) : 0);
                return result;
            }

            @Override
            public String toString() {
//                String scoreStr = String.format("%d: --(%.2f)--> %d", uid, score, newPid);
                return String.format("%3d: --(%.2f)--> %3d", userId, score, partitionId);//uid + ": --(" + score + ")-->" + newPid;
            }
        }

        NavigableSet<Score> scores = new TreeSet<Score>();
        for (Integer userId : masterIds) {
            SpajaUser user = manager.getUserMasterById(userId);
            for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
                scores.add(new Score(userId, replicaPartitionId, scoreReplicaPromotion(user, replicaPartitionId)));
            }
        }

        Map<Integer, Integer> remainingSpotsInPartitions = getRemainingSpotsInPartitions(partitionIdsToSkip);

        Map<Integer, Integer> strategy = new HashMap<Integer, Integer>();

        for (Iterator<Score> iter = scores.descendingIterator(); iter.hasNext(); ) {
            Score score = iter.next();
            int remainingSpotsInPartition = remainingSpotsInPartitions.get(score.partitionId);
            if (!strategy.containsKey(score.userId) && remainingSpotsInPartition > 0) {
                strategy.put(score.userId, score.partitionId);
                remainingSpotsInPartitions.put(score.partitionId, remainingSpotsInPartition - 1);
            }
        }

        Set<Integer> usersYetUnplaced = new HashSet<Integer>(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        for (Integer uid : usersYetUnplaced) {
            strategy.put(uid, getLeastOverloadedPartitionWhereThisUserHasAReplica(uid, strategy, manager.getAllPartitionIds()));
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

        return ((float) (numFriendsOnPartition * numFriendsOnPartition)) / (numFriendsTotal);
    }

    Map<Integer, Integer> getRemainingSpotsInPartitions(Set<Integer> partitionIdsToSkip) {
        int numUsers = manager.getNumUsers();
        int numPartitions = manager.getAllPartitionIds().size() - partitionIdsToSkip.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        Map<Integer, Integer> partitionToNumMastersMap = new HashMap<Integer, Integer>();
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (partitionIdsToSkip.contains(partitionId)) {
                continue;
            }
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
