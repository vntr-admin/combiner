package io.vntr.spj2;

import io.vntr.utils.ProbabilityUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;
import static io.vntr.Utils.generateBidirectionalFriendshipSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/18/17.
 */
public class SpJ2InitUtilsTest {
    @Test
    public void testInitGraph() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put(2,  initSet(3, 6, 9, 12));
        friendships.put(3,  initSet(4, 8, 12));
        friendships.put(4,  initSet(5, 10));
        friendships.put(5,  initSet(6, 12));
        friendships.put(6,  initSet(7));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        SpJ2Manager manager = SpJ2InitUtils.initGraph(1, 1, 2, 0.5f, 7, 0, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        assertEquals(partitions, manager.getPartitionToUserMap());
        assertEquals(bidirectionalFriendships, manager.getFriendships());
        assertEquals(replicas, manager.getPartitionToReplicasMap());

        int expectedCut = 13;

        for(Integer pid : partitions.keySet()) {
            SpJ2Partition partition = manager.getPartitionById(pid);
            assertEquals(partition.getIdsOfMasters(), partitions.get(pid));
            assertEquals(partition.getIdsOfReplicas(), replicas.get(pid));
            for(Integer uid : partition.getIdsOfMasters()) {
                assertEquals(manager.getUserMasterById(uid).getBasePid(), pid);
            }
            for(Integer uid : partition.getIdsOfReplicas()) {
                assertTrue(manager.getUserMasterById(uid).getReplicaPids().contains(pid));
            }
        }

        for(Integer uid : manager.getUids()) {
            assertEquals(manager.getUserMasterById(uid).getFriendIDs(), bidirectionalFriendships.get(uid));
            for(Integer replicaPid : manager.getUserMasterById(uid).getReplicaPids()) {
                assertTrue(replicas.get(replicaPid).contains(uid));
            }
        }

        assertTrue(manager.getMigrationTally() == 0);
        assertTrue(manager.getEdgeCut() == expectedCut);
        assertTrue(manager.getReplicationCount() == 18);
    }
}
