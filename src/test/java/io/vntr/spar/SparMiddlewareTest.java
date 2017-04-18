package io.vntr.spar;

import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
import static org.junit.Assert.*;

public class SparMiddlewareTest {

    @Test
    public void testPerformRebalace() {
        int minNumReplicas = 1;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 20));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(14));
        friendships.put(14, initSet(15));
        friendships.put(15, initSet(16));
        friendships.put(16, initSet(17));
        friendships.put(17, initSet(18));
        friendships.put(18, initSet(19));
        friendships.put(19, initSet(20));
        friendships.put(20, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<>();
        replicaPartitions.put(1, initSet( 6,  8,  9, 10, 12, 14, 15, 16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 11, 14, 16, 18, 19, 20));
        replicaPartitions.put(3, initSet( 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 16, 17));
        replicaPartitions.put(4, initSet( 1,  2,  3,  5,  7,  8,  9, 11, 12, 13, 14, 15));

        //No change test 1: no replicas added
        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        middleware.performRebalace(NO_CHANGE,  3, 19); //should not change anything, including replicas
        Map<Integer, Set<Integer>> mastersAfter   = manager.getPartitionToUserMap();
        Map<Integer, Set<Integer>> replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should not change replicas
        assertEquals(replicaPartitions, replicasAfter);


        //No change test 2: one replica added
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(NO_CHANGE,  7, 17); //should add 17 replica to p2
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17 to p2 and change nothing else
        assertFalse(replicaPartitions.get(2).contains(17));
        assertEquals(initSet(replicaPartitions.get(2), 17), replicasAfter.get(2));

        //Should change nothing else
        assertEquals(replicaPartitions.get(1), replicasAfter.get(1));
        assertEquals(replicaPartitions.get(3), replicasAfter.get(3));
        assertEquals(replicaPartitions.get(4), replicasAfter.get(4));


        //No change test 3: 2 replicas added
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(NO_CHANGE, 10, 17); //should add 17 replica to p2 and a copy of 10 to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17 to p2
        assertFalse(replicaPartitions.get(2).contains(17));
        assertEquals(initSet(replicaPartitions.get(2), 17), replicasAfter.get(2));

        //Should add 10 to p4
        assertFalse(replicaPartitions.get(4).contains(10));
        assertEquals(initSet(replicaPartitions.get(4), 10), replicasAfter.get(4));

        //Should change nothing else
        assertEquals(replicaPartitions.get(1), replicasAfter.get(1));
        assertEquals(replicaPartitions.get(3), replicasAfter.get(3));

        //Small to large test
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(SMALL_TO_LARGE, 1, 19); //should move 1 to p4; should remove a replica of 14 from p1; should remove a replica of 1 from p4; should add replicas of 4, 6, and 10 to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 1 to p4
        assertTrue(partitions.get(1).contains(4));
        assertFalse(partitions.get(4).contains(4));
        assertFalse(mastersAfter.get(1).contains(1));
        assertEquals(initSet(partitions.get(4), 1), mastersAfter.get(4));

        //Shouldn't change p2 or p3
        assertEquals(partitions.get(2), mastersAfter.get(2));
        assertEquals(partitions.get(3), mastersAfter.get(3));

        //Should remove a replica of 14 from p1
        assertTrue(replicaPartitions.get(1).contains(14));
        assertFalse(replicasAfter.get(1).contains(14));

        //Should remove a replica of 1 from p4
        assertTrue(replicaPartitions.get(4).contains(1));
        assertFalse(replicasAfter.get(4).contains(1));

        //Should add replicas of 4, 6, and 10 to p4
        assertFalse(replicaPartitions.get(4).contains(4));
        assertFalse(replicaPartitions.get(4).contains(6));
        assertFalse(replicaPartitions.get(4).contains(10));
        assertTrue(replicasAfter.get(4).containsAll(initSet(4, 6, 10)));

        //Shouldn't change p2 or p3
        assertEquals(replicaPartitions.get(2), replicasAfter.get(2));
        assertEquals(replicaPartitions.get(3), replicasAfter.get(3));


        //Large to small test
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(LARGE_TO_SMALL, 2, 11); //should move 11 to p1; should remove a replica of 10 from p3
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 11 to p1
        assertTrue(partitions.get(3).contains(11));
        assertFalse(partitions.get(1).contains(11));
        assertFalse(mastersAfter.get(3).contains(11));
        assertTrue(mastersAfter.get(1).contains(11));

        //Shouldn't change p2 or p4
        assertEquals(partitions.get(2), mastersAfter.get(2));
        assertEquals(partitions.get(4), mastersAfter.get(4));

        //Should remove a replica of 10 from p3
        assertTrue(replicaPartitions.get(3).contains(10));
        assertFalse(replicasAfter.get(3).contains(10));

        //Shouldn't change p1, p2 or p4
        assertEquals(replicaPartitions.get(1), replicasAfter.get(1));
        assertEquals(replicaPartitions.get(2), replicasAfter.get(2));
        assertEquals(replicaPartitions.get(4), replicasAfter.get(4));
    }



    @Test
    public void testUnfriend() {
        int minNumReplicas = 1;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 20));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(14));
        friendships.put(14, initSet(15));
        friendships.put(15, initSet(16));
        friendships.put(16, initSet(17));
        friendships.put(17, initSet(18));
        friendships.put(18, initSet(19));
        friendships.put(19, initSet(20));
        friendships.put(20, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 11, 18, 19));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15,  3,  5));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals((Integer) 28, middleware.getEdgeCut());

        assertEquals(manager.getUserMasterById(10).getReplicaPids(), initSet(3));
        assertEquals(manager.getUserMasterById(11).getReplicaPids(), initSet(2, 4));

        middleware.unfriend(10, 11);

        assertEquals(manager.getUserMasterById(10).getReplicaPids(), initSet(3));
        assertEquals(manager.getUserMasterById(11).getReplicaPids(), initSet(4));


        assertEquals(manager.getUserMasterById( 5).getReplicaPids(), initSet(2, 4));
        assertEquals(manager.getUserMasterById(18).getReplicaPids(), initSet(1, 2));

        middleware.unfriend(5, 18);

        assertEquals(manager.getUserMasterById( 5).getReplicaPids(), initSet(2));
        assertEquals(manager.getUserMasterById(18).getReplicaPids(), initSet(1, 2));
    }

//    @Test TODO: reenable this
    public void testRemovePartition() {
        int minNumReplicas = 1;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 20));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(14));
        friendships.put(14, initSet(15));
        friendships.put(15, initSet(16));
        friendships.put(16, initSet(17));
        friendships.put(17, initSet(18));
        friendships.put(18, initSet(19));
        friendships.put(19, initSet(20));
        friendships.put(20, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 19));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15,  3));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals((Integer) 28, middleware.getEdgeCut());

        middleware.removePartition(1);

        Map<Integer, Set<Integer>> expectedPMap = new HashMap<>();
        expectedPMap.put(2, initSet( 6,  7,  8,  9, 10,  1,  4,  5));
        expectedPMap.put(3, initSet(11, 12, 13, 14, 15,  2));
        expectedPMap.put(4, initSet(16, 17, 18, 19, 20,  3));

        Map<Integer, Set<Integer>> pMap = middleware.getPartitionToUserMap();

        assertEquals(pMap, expectedPMap);
    }

    @Test
    public void testDetermineUsersWhoWillNeedAnAdditionalReplica() {
        int minNumReplicas = 1;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 20));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(14));
        friendships.put(14, initSet(15));
        friendships.put(15, initSet(16));
        friendships.put(16, initSet(17));
        friendships.put(17, initSet(18));
        friendships.put(18, initSet(19));
        friendships.put(19, initSet(20));
        friendships.put(20, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 19));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15,  3));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals(initSet(1, 4, 5, 16, 18, 20), middleware.determineUsersWhoWillNeedAnAdditionalReplica(1));
    }

    @Test
    public void testGetRandomPartitionIdWhereThisUserIsNotPresent() {
        int minNumReplicas = 1;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 20));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(14));
        friendships.put(14, initSet(15));
        friendships.put(15, initSet(16));
        friendships.put(16, initSet(17));
        friendships.put(17, initSet(18));
        friendships.put(18, initSet(19));
        friendships.put(19, initSet(20));
        friendships.put(20, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        for(int uid=1; uid<20; uid++) {
            int pid = ((uid-1)/5) + 1;
            Set<Integer> possibleAnswers = initSet((pid+1)%4 + 1, (pid+2)%4 + 1);
            int newPid = middleware.getRandomPartitionIdWhereThisUserIsNotPresent(manager.getUserMasterById(uid), Collections.<Integer>emptySet());
            assertTrue(possibleAnswers.contains(newPid));
        }
    }
}