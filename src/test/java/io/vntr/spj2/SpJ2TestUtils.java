package io.vntr.spj2;

import io.vntr.TestUtils;
import io.vntr.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SpJ2TestUtils {
    public static final Integer[] STANDAR_USER_ID_ARRAY = {3, 4, 5, 6, 8, 10, 11, 12};

    public static class IntegerPair {
        public final Integer val1;
        public final Integer val2;
        private final String str;
        private final int hash;

        public IntegerPair(Integer val1, Integer val2) {
            this.val1 = val1;
            this.val2 = val2;
            this.str = val1 + ":" + val2;
            int val1PartOfHash = val1.shortValue();
            int val2PartOfHash = val2.shortValue();
            hash = oneTimeHashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IntegerPair that = (IntegerPair) o;

            if (hash != that.hash) return false;
            if (!val1.equals(that.val1)) return false;
            if (!val2.equals(that.val2)) return false;
            return str.equals(that.str);

        }

        public int oneTimeHashCode() {
            int result = val1.hashCode();
            result = 31 * result + val2.hashCode();
            result = 31 * result + str.hashCode();
            result = 31 * result + hash;
            return result;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    public static class GraphSpec {
        private Map<Integer, Set<Integer>> partitionToMastersMap;
        private Map<Integer, Set<Integer>> partitionToReplicasMap;
        private Map<Integer, Integer> userIdToMasterPartitionIdMap;
        private Map<Integer, Set<Integer>> userIdToReplicaPartitionIdsMap;
        private Map<Integer, Set<Integer>> userIdToFriendIdsMap;
        private List<IntegerPair> friendships;

        private boolean isInitialized = false;

        public GraphSpec() {
            partitionToMastersMap = new HashMap<>();
            partitionToReplicasMap = new HashMap<>();
            friendships = new LinkedList<>();
            userIdToMasterPartitionIdMap = new HashMap<>();
            userIdToReplicaPartitionIdsMap = new HashMap<>();
            userIdToFriendIdsMap = new HashMap<>();
        }

        public void init() {
            if (isInitialized) {
                return;
            }
            isInitialized = true;

            for (Integer partitionId : partitionToMastersMap.keySet()) {
                for (Integer userId : partitionToMastersMap.get(partitionId)) {
                    userIdToMasterPartitionIdMap.put(userId, partitionId);
                }
            }

            for (Integer userId : userIdToMasterPartitionIdMap.keySet()) {
                userIdToReplicaPartitionIdsMap.put(userId, new HashSet<Integer>());
                userIdToFriendIdsMap.put(userId, new HashSet<Integer>());
            }

            for (Integer partitionId : partitionToReplicasMap.keySet()) {
                for (Integer userId : partitionToReplicasMap.get(partitionId)) {
                    if (!userIdToReplicaPartitionIdsMap.containsKey(userId)) {
                        userIdToReplicaPartitionIdsMap.put(userId, new HashSet<Integer>());
                    }

                    userIdToReplicaPartitionIdsMap.get(userId).add(partitionId);
                }
            }

            for (IntegerPair friendship : friendships) {
                if (!userIdToFriendIdsMap.containsKey(friendship.val1)) {
                    userIdToFriendIdsMap.put(friendship.val1, new HashSet<Integer>());
                }
                if (!userIdToFriendIdsMap.containsKey(friendship.val2)) {
                    userIdToFriendIdsMap.put(friendship.val2, new HashSet<Integer>());
                }
                userIdToFriendIdsMap.get(friendship.val1).add(friendship.val2);
                userIdToFriendIdsMap.get(friendship.val2).add(friendship.val1);
            }
        }

        public void addPartitionMastersPairing(Integer partitionId, Set<Integer> masterIds) {
            partitionToMastersMap.put(partitionId, masterIds);
        }

        public void addPartitionReplicasPairing(Integer partitionId, Set<Integer> replicaIds) {
            partitionToReplicasMap.put(partitionId, replicaIds);
        }

        public void addFriendship(Integer friendId1, Integer friendId2) {
            friendships.add(new IntegerPair(friendId1, friendId2));
        }

        public Map<Integer, Set<Integer>> getPartitionsToMastersMap() {
            return Collections.unmodifiableMap(partitionToMastersMap);
        }

        public Map<Integer, Set<Integer>> getPartitionsToReplicasMap() {
            return Collections.unmodifiableMap(partitionToReplicasMap);
        }

        public List<IntegerPair> getFriendships() {
            return Collections.unmodifiableList(friendships);
        }

        public Integer getMasterPartitionIdForUser(Integer userId) {
            return userIdToMasterPartitionIdMap.get(userId);
        }

        public Set<Integer> getReplicaPartitionIdsForUser(Integer userId) {
            return Collections.unmodifiableSet(userIdToReplicaPartitionIdsMap.get(userId));
        }

        public Set<Integer> getFriendIdsForUser(Integer userId) {
            return userIdToFriendIdsMap.get(userId);
        }

        public Set<Integer> getUserIds() {
            return Collections.unmodifiableSet(userIdToMasterPartitionIdMap.keySet());
        }
    }

    public static SpJ2Manager getStandardManager() {
        SpJ2Manager manager = new SpJ2Manager(2, 1f, 2, 0.5f, 15, 0);

        for (int i = 0; i < 5; i++) {
            manager.addPartition();
        }

        for (int i = 0; i < 8; i++) {
            manager.addUser(new User(STANDAR_USER_ID_ARRAY[i]));
        }

        return manager;
    }

    public static Set<Integer> getUserIdsNotInPartition(SpJ2Manager manager, Integer partitionId) {
        Set<Integer> userIdsNotInPartition = new HashSet<>();
        SpJ2Partition partition = manager.getPartitionById(partitionId);
        for (Integer userId : manager.getAllUserIds()) {
            if (!partition.getIdsOfMasters().contains(userId) && !partition.getIdsOfReplicas().contains(userId)) {
                userIdsNotInPartition.add(userId);
            }
        }

        return userIdsNotInPartition;
    }

    public static Set<Integer> getColocatedUserIds(SpJ2Manager manager, Integer userId) {
        Set<Integer> userIds = new HashSet<>(manager.getAllUserIds());
        userIds.removeAll(getNonColocatedUserIds(manager, userId));
        return userIds;
    }

    public static Set<Integer> getNonColocatedUserIds(SpJ2Manager manager, Integer userId) {
        Set<Integer> nonColocatedUserIds = new HashSet<>();

        Set<Integer> otherUsersPartitions = getPartitionsWithAPresence(manager, userId);
        for (Integer curUserId : manager.getAllUserIds()) {
            Set<Integer> distinctPartitions = new HashSet<>(otherUsersPartitions);
            int initialSize = distinctPartitions.size();
            distinctPartitions.removeAll(getPartitionsWithAPresence(manager, curUserId));
            int currentSize = distinctPartitions.size();
            if (initialSize == currentSize) {
                nonColocatedUserIds.add(curUserId);
            }
        }

        return nonColocatedUserIds;
    }

    public static Set<Integer> getPartitionsWithAPresence(SpJ2Manager manager, Integer userId) {
        SpJ2User user = manager.getUserMasterById(userId);
        Set<Integer> partitionsWithAPresence = new HashSet<>(user.getReplicaPids());
        partitionsWithAPresence.add(user.getMasterPid());
        return partitionsWithAPresence;
    }

    public static Set<Integer> getPartitionsWithNoPresence(SpJ2Manager manager, Integer userId) {
        Set<Integer> partitionsWithoutAPresence = new HashSet<>(manager.getAllPartitionIds());
        partitionsWithoutAPresence.removeAll(getPartitionsWithAPresence(manager, userId));
        return partitionsWithoutAPresence;
    }

    public static SpJ2User getUserWithMasterOnPartition(SpJ2Manager manager, Integer partitionId) {
        SpJ2Partition partition = manager.getPartitionById(partitionId);
        Integer userId = partition.getIdsOfMasters().iterator().next();
        return manager.getUserMasterById(userId);
    }

    public static Set<Integer> getPartitionIdsWithNMasters(SpJ2Manager manager, int n) {
        Set<Integer> partitionIdsWithNMasters = new HashSet<>();
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (manager.getPartitionById(partitionId).getNumMasters() == n) {
                partitionIdsWithNMasters.add(partitionId);
            }
        }

        return partitionIdsWithNMasters;
    }

}