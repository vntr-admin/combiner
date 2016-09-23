package io.vntr.spar;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.vntr.spar.TestUtils.GraphSpec;
import io.vntr.spar.TestUtils.LongPair;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestUtilsTest {
    @Test
    public void testBuildGraphFromGraphSpec() {
        Set<Long> allUserIds = new HashSet<Long>(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));

        Map<Long, Set<Long>> partitionIdToMastersMap = new HashMap<Long, Set<Long>>();
        partitionIdToMastersMap.put(1L, new HashSet<Long>(Arrays.asList(1L, 10L)));
        partitionIdToMastersMap.put(2L, new HashSet<Long>(Arrays.asList(2L, 3L)));
        partitionIdToMastersMap.put(3L, new HashSet<Long>(Arrays.asList(4L, 5L)));
        partitionIdToMastersMap.put(4L, new HashSet<Long>(Arrays.asList(6L, 7L)));
        partitionIdToMastersMap.put(5L, new HashSet<Long>(Arrays.asList(8L, 9L)));

        Map<Long, Set<Long>> partitionIdToReplicasMap = new HashMap<Long, Set<Long>>();
        partitionIdToReplicasMap.put(1L, new HashSet<Long>(Arrays.asList(3L, 6L, 7L, 8L)));
        partitionIdToReplicasMap.put(2L, new HashSet<Long>(Arrays.asList(1L, 5L, 8L, 9L)));
        partitionIdToReplicasMap.put(3L, new HashSet<Long>(Arrays.asList(1L, 10L, 2L, 7L)));
        partitionIdToReplicasMap.put(4L, new HashSet<Long>(Arrays.asList(2L, 3L, 4L, 9L)));
        partitionIdToReplicasMap.put(5L, new HashSet<Long>(Arrays.asList(10L, 4L, 5L, 6L)));

        Set<LongPair> friendships = new HashSet<TestUtils.LongPair>();
        friendships.add(new LongPair(1L, 2L));
        friendships.add(new LongPair(1L, 4L));
        friendships.add(new LongPair(1L, 8L));
        friendships.add(new LongPair(2L, 5L));
        friendships.add(new LongPair(3L, 7L));
        friendships.add(new LongPair(4L, 8L));
        friendships.add(new LongPair(6L, 9L));

        int minNumReplicas = 2;

        GraphSpec spec = new GraphSpec();

        for (Long partitionId : partitionIdToMastersMap.keySet()) {
            spec.addPartitionMastersPairing(partitionId, partitionIdToMastersMap.get(partitionId));
            spec.addPartitionReplicasPairing(partitionId, partitionIdToReplicasMap.get(partitionId));
        }

        for (LongPair friendship : friendships) {
            spec.addFriendship(friendship.val1, friendship.val2);
        }

        SparManager manager = TestUtils.buildGraphFromGraphSpec(spec, minNumReplicas);

        assertTrue(manager.getMinNumReplicas() == minNumReplicas);
        assertEquals(new HashSet<Long>(partitionIdToMastersMap.keySet()), new HashSet<Long>(manager.getAllPartitionIds()));
        assertEquals(allUserIds, new HashSet<Long>(manager.getAllUserIds()));

        //Ensure all friendships are present
        for (LongPair friendship : friendships) {
            SparUser user1 = manager.getUserMasterById(friendship.val1);
            SparUser user2 = manager.getUserMasterById(friendship.val2);
            assertTrue(user1.getFriendIDs().contains(friendship.val2));
            assertTrue(user2.getFriendIDs().contains(friendship.val1));
            for (Long replicaId : user1.getReplicaPartitionIds()) {
                SparUser user1Replica = manager.getPartitionById(replicaId).getReplicaById(friendship.val1);
                assertTrue(user1Replica.getFriendIDs().contains(friendship.val2));
            }
            for (Long replicaId : user2.getReplicaPartitionIds()) {
                SparUser user2Replica = manager.getPartitionById(replicaId).getReplicaById(friendship.val2);
                assertTrue(user2Replica.getFriendIDs().contains(friendship.val1));
            }
        }

        for (Long userId : allUserIds) {
            SparUser user = manager.getUserMasterById(userId);
            for (Long friendId : user.getFriendIDs()) {
                assertTrue(friendships.contains(new LongPair(userId, friendId)) || friendships.contains(new LongPair(friendId, userId)));
            }
        }

        //Ensure that masters and replicas are placed in the correct partition
        for (Long userId : allUserIds) {
            SparUser user = manager.getUserMasterById(userId);
            Long masterPartitionId = user.getMasterPartitionId();
            assertTrue(partitionIdToMastersMap.get(masterPartitionId).contains(userId));
            for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
                assertTrue(partitionIdToReplicasMap.get(replicaPartitionId).contains(userId));
            }
        }

        for (Long partitionId : partitionIdToMastersMap.keySet()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            assertEquals(new HashSet<Long>(partition.getIdsOfMasters()), partitionIdToMastersMap.get(partitionId));
            assertEquals(new HashSet<Long>(partition.getIdsOfReplicas()), partitionIdToReplicasMap.get(partitionId));
        }
    }
}