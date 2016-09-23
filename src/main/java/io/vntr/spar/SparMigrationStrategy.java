package io.vntr.spar;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

public class SparMigrationStrategy {
    private SparManager manager;

    public SparMigrationStrategy(SparManager manager) {
        this.manager = manager;
    }

    public Map<Long, Long> getUserMigrationStrategy(Long partitionId) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        Set<Long> partitionIdsToSkip = new HashSet<Long>(Arrays.asList(partitionId));
        SparPartition partition = manager.getPartitionById(partitionId);
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
        }

        NavigableSet<Score> scores = new TreeSet<Score>();
        for (Long userId : masterIds) {
            SparUser user = manager.getUserMasterById(userId);
            for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
                scores.add(new Score(userId, replicaPartitionId, scoreReplicaPromotion(user, replicaPartitionId)));
            }
        }

        Map<Long, Integer> remainingSpotsInPartitions = getRemainingSpotsInPartitions(partitionIdsToSkip);

        Map<Long, Long> strategy = new HashMap<Long, Long>();

        Iterator<Score> iter = scores.descendingIterator();
        while (iter.hasNext()) {
            Score score = iter.next();
            int remainingSpotsInPartition = remainingSpotsInPartitions.get(score.partitionId);
            if (!strategy.containsKey(score.userId) && remainingSpotsInPartition > 0) {
                strategy.put(score.userId, score.partitionId);
                remainingSpotsInPartitions.put(score.partitionId, remainingSpotsInPartition - 1);
            }
        }

        Set<Long> usersYetUnplaced = new HashSet<Long>(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        List<Long> waterFillingStrategyRemainingPartitions = getWaterFillingStrategyOfPartitions(partitionIdsToSkip, strategy, usersYetUnplaced.size() + 1);
        Iterator<Long> waterFillingStrategyRemainingPartitionsIter = waterFillingStrategyRemainingPartitions.iterator();

        for (Long unplacedUserId : usersYetUnplaced) {
            strategy.put(unplacedUserId, waterFillingStrategyRemainingPartitionsIter.next());
        }

        return strategy;
    }

    double scoreReplicaPromotion(SparUser user, Long replicaPartitionId) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = 0;
        for (Long friendId : user.getFriendIDs()) {
            SparUser friend = manager.getUserMasterById(friendId);
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
            SparPartition partition = manager.getPartitionById(partitionId);
            partitionToNumMastersMap.put(partition.getId(), partition.getNumMasters());
        }

        Map<Long, Integer> remainingSpotsInPartitions = new HashMap<Long, Integer>();
        for (Long partitionId : partitionToNumMastersMap.keySet()) {
            remainingSpotsInPartitions.put(partitionId, maxUsersPerPartition - partitionToNumMastersMap.get(partitionId));
        }

        return remainingSpotsInPartitions;
    }

    List<Long> getWaterFillingStrategyOfPartitions(Set<Long> partitionIdsToSkip, Map<Long, Long> currentStrategy, int maxListLength) {
        class SizedPartition implements Comparable<SizedPartition> {
            public final long partitionId;
            private int numMasters;

            public SizedPartition(long partitionId, int numMasters) {
                this.partitionId = partitionId;
                this.numMasters = numMasters;
            }

            @Override
            public String toString() {
                return "P" + partitionId + ":" + numMasters;
            }

            public int getNumMasters() {
                return numMasters;
            }

            @SuppressWarnings("unused")
            public void setNumMasters(int numMasters) {
                this.numMasters = numMasters;
            }

            @Override
            public int hashCode() {
                return (int) partitionId;
            }

            @Override
            public boolean equals(Object o) {
                if (o instanceof SizedPartition) {
                    SizedPartition s = (SizedPartition) o;
                    return s.partitionId == this.partitionId && s.numMasters == this.numMasters;
                }
                return false;
            }

            public int compareTo(SizedPartition o) {
                if (this.numMasters > o.numMasters) {
                    return 1;
                } else if (this.numMasters < o.numMasters) {
                    return -1;
                } else {
                    if (this.partitionId > o.partitionId) {
                        return 1;
                    } else if (this.partitionId < o.partitionId) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }
        }

        Map<Long, Integer> partitionIdToNumMastersMap = new HashMap<Long, Integer>();
        for (Long partitionId : manager.getAllPartitionIds()) {
            if (partitionIdsToSkip.contains(partitionId)) {
                continue;
            }
            partitionIdToNumMastersMap.put(partitionId, manager.getPartitionById(partitionId).getNumMasters());
        }

        for (Long key : currentStrategy.keySet()) {
            Long partitionId = currentStrategy.get(key);
            int currentCount = partitionIdToNumMastersMap.get(partitionId);
            partitionIdToNumMastersMap.put(partitionId, currentCount + 1);
        }

        PriorityQueue<SizedPartition> queue = new PriorityQueue<SizedPartition>();

        for (Long partitionId : partitionIdToNumMastersMap.keySet()) {
            queue.add(new SizedPartition(partitionId, partitionIdToNumMastersMap.get(partitionId)));
        }

        List<Long> partitionList = new LinkedList<Long>();

        while (partitionList.size() < maxListLength) {
            SizedPartition s = queue.poll();
            partitionList.add(s.partitionId);
            queue.add(new SizedPartition(s.partitionId, s.getNumMasters() + 1));
        }

        return partitionList;
    }
}