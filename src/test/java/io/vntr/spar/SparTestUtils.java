package io.vntr.spar;

import io.vntr.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparTestUtils {
    public static final Long[]   STANDAR_USER_ID_ARRAY =   {    3L,          4L,       5L,      6L,       8L,         10L,    11L,   12L};
    public static final String[] STANDARD_USER_NAME_ARRAY = {"Greg", "Henrietta", "Imogen", "James", "Kartuk", "Llewellyn", "Ming", "Nao"};

    public static class LongPair {
        public final Long val1;
        public final Long val2;
        private final String str;
        private final int hash;

        public LongPair(Long val1, Long val2) {
            this.val1 = val1;
            this.val2 = val2;
            this.str = val1 + ":" + val2;
            int val1PartOfHash = val1.shortValue();
            int val2PartOfHash = val2.shortValue();
            hash = val1PartOfHash << 16 + val2PartOfHash;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof LongPair) {
                LongPair lp = (LongPair) o;
                return lp.val1.longValue() == this.val1.longValue() && lp.val2.longValue() == this.val2.longValue();
            }
            return false;
        }

        @Override
        public String toString() {
            return str;
        }
    }

    public static class GraphSpec {
        private Map<Long, Set<Long>> partitionToMastersMap;
        private Map<Long, Set<Long>> partitionToReplicasMap;
        private Map<Long, Long> userIdToMasterPartitionIdMap;
        private Map<Long, Set<Long>> userIdToReplicaPartitionIdsMap;
        private Map<Long, Set<Long>> userIdToFriendIdsMap;
        private List<LongPair> friendships;

        private boolean isInitialized = false;

        public GraphSpec() {
            partitionToMastersMap = new HashMap<Long, Set<Long>>();
            partitionToReplicasMap = new HashMap<Long, Set<Long>>();
            friendships = new LinkedList<LongPair>();
            userIdToMasterPartitionIdMap = new HashMap<Long, Long>();
            userIdToReplicaPartitionIdsMap = new HashMap<Long, Set<Long>>();
            userIdToFriendIdsMap = new HashMap<Long, Set<Long>>();
        }

        public void init() {
            if (isInitialized) {
                return;
            }
            isInitialized = true;

            for (Long partitionId : partitionToMastersMap.keySet()) {
                for (Long userId : partitionToMastersMap.get(partitionId)) {
                    userIdToMasterPartitionIdMap.put(userId, partitionId);
                }
            }

            for (Long userId : userIdToMasterPartitionIdMap.keySet()) {
                userIdToReplicaPartitionIdsMap.put(userId, new HashSet<Long>());
                userIdToFriendIdsMap.put(userId, new HashSet<Long>());
            }

            for (Long partitionId : partitionToReplicasMap.keySet()) {
                for (Long userId : partitionToReplicasMap.get(partitionId)) {
                    if (!userIdToReplicaPartitionIdsMap.containsKey(userId)) {
                        userIdToReplicaPartitionIdsMap.put(userId, new HashSet<Long>());
                    }

                    userIdToReplicaPartitionIdsMap.get(userId).add(partitionId);
                }
            }

            for (LongPair friendship : friendships) {
                if (!userIdToFriendIdsMap.containsKey(friendship.val1)) {
                    userIdToFriendIdsMap.put(friendship.val1, new HashSet<Long>());
                }
                if (!userIdToFriendIdsMap.containsKey(friendship.val2)) {
                    userIdToFriendIdsMap.put(friendship.val2, new HashSet<Long>());
                }
                userIdToFriendIdsMap.get(friendship.val1).add(friendship.val2);
                userIdToFriendIdsMap.get(friendship.val2).add(friendship.val1);
            }
        }

        public void addPartitionMastersPairing(Long partitionId, Set<Long> masterIds) {
            partitionToMastersMap.put(partitionId, masterIds);
        }

        public void addPartitionReplicasPairing(Long partitionId, Set<Long> replicaIds) {
            partitionToReplicasMap.put(partitionId, replicaIds);
        }

        public void addFriendship(Long friendId1, Long friendId2) {
            friendships.add(new LongPair(friendId1, friendId2));
        }

        public Map<Long, Set<Long>> getPartitionsToMastersMap() {
            return Collections.unmodifiableMap(partitionToMastersMap);
        }

        public Map<Long, Set<Long>> getPartitionsToReplicasMap() {
            return Collections.unmodifiableMap(partitionToReplicasMap);
        }

        public List<LongPair> getFriendships() {
            return Collections.unmodifiableList(friendships);
        }

        public Long getMasterPartitionIdForUser(Long userId) {
            return userIdToMasterPartitionIdMap.get(userId);
        }

        public Set<Long> getReplicaPartitionIdsForUser(Long userId) {
            return Collections.unmodifiableSet(userIdToReplicaPartitionIdsMap.get(userId));
        }

        public Set<Long> getFriendIdsForUser(Long userId) {
            return userIdToFriendIdsMap.get(userId);
        }

        public Set<Long> getUserIds() {
            return Collections.unmodifiableSet(userIdToMasterPartitionIdMap.keySet());
        }
    }

    public static SparManager buildGraphFromGraphSpec(GraphSpec spec, int minNumReplicas) {
        spec.init();
        SparManager manager = new SparManager(minNumReplicas);

        for (Long partitionId : spec.getPartitionsToMastersMap().keySet()) {
            manager.addPartition(partitionId);
        }

        for (Long userId : spec.getUserIds()) {
            Long masterPartitionId = spec.getMasterPartitionIdForUser(userId);
            Set<Long> replicaPartitionIds = spec.getReplicaPartitionIdsForUser(userId);
            SparUser user = new SparUser("user" + userId, userId);
            user.setMasterPartitionId(masterPartitionId);
            user.setPartitionId(masterPartitionId);
            user.addReplicaPartitionIds(replicaPartitionIds);
            for (Long friendId : spec.getFriendIdsForUser(userId)) {
                user.befriend(friendId);
            }
            manager.addUser(user, masterPartitionId);
            for (Long replicaPartitionId : replicaPartitionIds) {
                manager.addReplicaNoUpdates(user, replicaPartitionId);
            }
        }

        return manager;
    }

    public static SparManager getStandardManager() {
        SparManager manager = new SparManager(2);

        for (int i = 0; i < 5; i++) {
            manager.addPartition();
        }

        for (int i = 0; i < 8; i++) {
            manager.addUser(new User(STANDARD_USER_NAME_ARRAY[i], STANDAR_USER_ID_ARRAY[i]));
        }

        return manager;
    }

    public static Set<Long> getUserIdsNotInPartition(SparManager manager, Long partitionId) {
        Set<Long> userIdsNotInPartition = new HashSet<Long>();
        SparPartition partition = manager.getPartitionById(partitionId);
        for (Long userId : manager.getAllUserIds()) {
            if (!partition.getIdsOfMasters().contains(userId) && !partition.getIdsOfReplicas().contains(userId)) {
                userIdsNotInPartition.add(userId);
            }
        }

        return userIdsNotInPartition;
    }

    public static Set<Long> getColocatedUserIds(SparManager manager, Long userId) {
        Set<Long> userIds = new HashSet<Long>(manager.getAllUserIds());
        userIds.removeAll(getNonColocatedUserIds(manager, userId));
        return userIds;
    }

    public static Set<Long> getNonColocatedUserIds(SparManager manager, Long userId) {
        Set<Long> nonColocatedUserIds = new HashSet<Long>();

        Set<Long> otherUsersPartitions = getPartitionsWithAPresence(manager, userId);
        for (Long curUserId : manager.getAllUserIds()) {
            Set<Long> distinctPartitions = new HashSet<Long>(otherUsersPartitions);
            int initialSize = distinctPartitions.size();
            distinctPartitions.removeAll(getPartitionsWithAPresence(manager, curUserId));
            int currentSize = distinctPartitions.size();
            if (initialSize == currentSize) {
                nonColocatedUserIds.add(curUserId);
            }
        }

        return nonColocatedUserIds;
    }

    public static Set<Long> getPartitionsWithAPresence(SparManager manager, Long userId) {
        SparUser user = manager.getUserMasterById(userId);
        Set<Long> partitionsWithAPresence = new HashSet<Long>(user.getReplicaPartitionIds());
        partitionsWithAPresence.add(user.getMasterPartitionId());
        return partitionsWithAPresence;
    }

    public static Set<Long> getPartitionsWithNoPresence(SparManager manager, Long userId) {
        Set<Long> partitionsWithoutAPresence = new HashSet<Long>(manager.getAllPartitionIds());
        partitionsWithoutAPresence.removeAll(getPartitionsWithAPresence(manager, userId));
        return partitionsWithoutAPresence;
    }

    public static SparUser getUserWithMasterOnPartition(SparManager manager, Long partitionId) {
        SparPartition partition = manager.getPartitionById(partitionId);
        Long userId = partition.getIdsOfMasters().iterator().next();
        return manager.getUserMasterById(userId);
    }

    public static Set<Long> getPartitionIdsWithNMasters(SparManager manager, int n) {
        Set<Long> partitionIdsWithNMasters = new HashSet<Long>();
        for (Long partitionId : manager.getAllPartitionIds()) {
            if (manager.getPartitionById(partitionId).getNumMasters() == n) {
                partitionIdsWithNMasters.add(partitionId);
            }
        }

        return partitionIdsWithNMasters;
    }
}