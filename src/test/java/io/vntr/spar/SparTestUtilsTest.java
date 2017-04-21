package io.vntr.spar;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.vntr.RepUser;
import io.vntr.spar.SparTestUtils.GraphSpec;
import io.vntr.spar.SparTestUtils.IntegerPair;

import org.junit.Test;

import static org.junit.Assert.*;

public class SparTestUtilsTest {
    @Test
    public void testBuildGraphFromGraphSpec() {
        Set<Integer> allUserIds = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        Map<Integer, Set<Integer>> partitionIdToMastersMap = new HashMap<>();
        partitionIdToMastersMap.put(1, new HashSet<>(Arrays.asList(1, 10)));
        partitionIdToMastersMap.put(2, new HashSet<>(Arrays.asList(2, 3)));
        partitionIdToMastersMap.put(3, new HashSet<>(Arrays.asList(4, 5)));
        partitionIdToMastersMap.put(4, new HashSet<>(Arrays.asList(6, 7)));
        partitionIdToMastersMap.put(5, new HashSet<>(Arrays.asList(8, 9)));

        Map<Integer, Set<Integer>> partitionIdToReplicasMap = new HashMap<>();
        partitionIdToReplicasMap.put(1, new HashSet<>(Arrays.asList(3, 6, 7, 8)));
        partitionIdToReplicasMap.put(2, new HashSet<>(Arrays.asList(1, 5, 8, 9)));
        partitionIdToReplicasMap.put(3, new HashSet<>(Arrays.asList(1, 10, 2, 7)));
        partitionIdToReplicasMap.put(4, new HashSet<>(Arrays.asList(2, 3, 4, 9)));
        partitionIdToReplicasMap.put(5, new HashSet<>(Arrays.asList(10, 4, 5, 6)));

        Set<IntegerPair> friendships = new HashSet<>();
        friendships.add(new IntegerPair(1, 2));
        friendships.add(new IntegerPair(1, 4));
        friendships.add(new IntegerPair(1, 8));
        friendships.add(new IntegerPair(2, 5));
        friendships.add(new IntegerPair(3, 7));
        friendships.add(new IntegerPair(4, 8));
        friendships.add(new IntegerPair(6, 9));

        int minNumReplicas = 2;

        GraphSpec spec = new GraphSpec();

        for (Integer partitionId : partitionIdToMastersMap.keySet()) {
            spec.addPartitionMastersPairing(partitionId, partitionIdToMastersMap.get(partitionId));
            spec.addPartitionReplicasPairing(partitionId, partitionIdToReplicasMap.get(partitionId));
        }

        for (IntegerPair friendship : friendships) {
            spec.addFriendship(friendship.val1, friendship.val2);
        }

        SparManager manager = SparTestUtils.buildGraphFromGraphSpec(spec, minNumReplicas);

        assertTrue(manager.getMinNumReplicas() == minNumReplicas);
        assertEquals(new HashSet<>(partitionIdToMastersMap.keySet()), new HashSet<>(manager.getAllPartitionIds()));
        assertEquals(allUserIds, new HashSet<>(manager.getAllUserIds()));

        //Ensure all friendships are present
        for (IntegerPair friendship : friendships) {
            RepUser user1 = manager.getUserMasterById(friendship.val1);
            RepUser user2 = manager.getUserMasterById(friendship.val2);
            assertTrue(user1.getFriendIDs().contains(friendship.val2));
            assertTrue(user2.getFriendIDs().contains(friendship.val1));
            for (Integer replicaId : user1.getReplicaPids()) {
                RepUser user1Replica = manager.getPartitionById(replicaId).getReplicaById(friendship.val1);
                assertTrue(user1Replica.getFriendIDs().contains(friendship.val2));
            }
            for (Integer replicaId : user2.getReplicaPids()) {
                RepUser user2Replica = manager.getPartitionById(replicaId).getReplicaById(friendship.val2);
                assertTrue(user2Replica.getFriendIDs().contains(friendship.val1));
            }
        }

        for (Integer userId : allUserIds) {
            RepUser user = manager.getUserMasterById(userId);
            for (Integer friendId : user.getFriendIDs()) {
                assertTrue(friendships.contains(new IntegerPair(userId, friendId)) || friendships.contains(new IntegerPair(friendId, userId)));
            }
        }

        //Ensure that masters and replicas are placed in the correct partition
        for (Integer userId : allUserIds) {
            RepUser user = manager.getUserMasterById(userId);
            Integer masterPartitionId = user.getBasePid();
            assertTrue(partitionIdToMastersMap.get(masterPartitionId).contains(userId));
            for (Integer replicaPartitionId : user.getReplicaPids()) {
                assertTrue(partitionIdToReplicasMap.get(replicaPartitionId).contains(userId));
            }
        }

        for (Integer partitionId : partitionIdToMastersMap.keySet()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            assertEquals(new HashSet<>(partition.getIdsOfMasters()), partitionIdToMastersMap.get(partitionId));
            assertEquals(new HashSet<>(partition.getIdsOfReplicas()), partitionIdToReplicasMap.get(partitionId));
        }
    }
}