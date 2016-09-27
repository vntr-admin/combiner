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
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L, 10L));
        partitions.put(3L, initSet(11L, 12L, 13L, 14L, 15L));
        partitions.put(4L, initSet(16L, 17L, 18L, 19L, 20L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L, 15L, 18L));
        friendships.put( 3L, initSet( 4L,  8L, 12L, 16L, 20L));
        friendships.put( 4L, initSet( 5L, 10L, 15L));
        friendships.put( 5L, initSet( 6L, 12L, 18L));
        friendships.put( 6L, initSet( 7L, 14L));
        friendships.put( 7L, initSet( 8L, 16L));
        friendships.put( 8L, initSet( 9L, 18L));
        friendships.put( 9L, initSet(10L, 20L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, initSet(14L));
        friendships.put(14L, initSet(15L));
        friendships.put(15L, initSet(16L));
        friendships.put(16L, initSet(17L));
        friendships.put(17L, initSet(18L));
        friendships.put(18L, initSet(19L));
        friendships.put(19L, initSet(20L));
        friendships.put(20L, Collections.<Long>emptySet());

        Map<Long, Set<Long>> replicaPartitions = new HashMap<Long, Set<Long>>();
        replicaPartitions.put(1L, initSet( 6L,  8L,  9L, 10L, 12L, 14L, 15L, 16L, 17L, 18L, 19L, 20L));
        replicaPartitions.put(2L, initSet( 1L,  2L,  3L,  4L,  5L, 11L, 14L, 16L, 18L, 19L, 20L));
        replicaPartitions.put(3L, initSet( 1L,  2L,  3L,  4L,  5L,  6L,  7L,  8L,  9L, 10L, 16L, 17L));
        replicaPartitions.put(4L, initSet( 1L,  2L,  3L,  5L,  7L,  8L,  9L, 11L, 12L, 13L, 14L, 15L));

        //No change test 1: no replicas added
        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        middleware.performRebalace(NO_CHANGE,  3L, 19L); //should not change anything, including replicas
        Map<Long, Set<Long>> mastersAfter   = manager.getPartitionToUserMap();
        Map<Long, Set<Long>> replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should not change replicas
        assertEquals(replicaPartitions, replicasAfter);


        //No change test 2: one replica added
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(NO_CHANGE,  7L, 17L); //should add 17L replica to p2
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17L to p2 and change nothing else
        assertFalse(replicaPartitions.get(2L).contains(17L));
        assertEquals(initSet(replicaPartitions.get(2L), 17L), replicasAfter.get(2L));

        //Should change nothing else
        assertEquals(replicaPartitions.get(1L), replicasAfter.get(1L));
        assertEquals(replicaPartitions.get(3L), replicasAfter.get(3L));
        assertEquals(replicaPartitions.get(4L), replicasAfter.get(4L));


        //No change test 3: 2 replicas added
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(NO_CHANGE, 10L, 17L); //should add 17L replica to p2 and a copy of 10L to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17L to p2
        assertFalse(replicaPartitions.get(2L).contains(17L));
        assertEquals(initSet(replicaPartitions.get(2L), 17L), replicasAfter.get(2L));

        //Should add 10L to p4
        assertFalse(replicaPartitions.get(4L).contains(10L));
        assertEquals(initSet(replicaPartitions.get(4L), 10L), replicasAfter.get(4L));

        //Should change nothing else
        assertEquals(replicaPartitions.get(1L), replicasAfter.get(1L));
        assertEquals(replicaPartitions.get(3L), replicasAfter.get(3L));

        //Small to large test
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(SMALL_TO_LARGE, 1L, 19L); //should move 1L to p4; should remove a replica of 14L from p1; should remove a replica of 1L from p4; should add replicas of 4L, 6L, and 10L to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 1L to p4
        assertTrue(partitions.get(1L).contains(4L));
        assertFalse(partitions.get(4L).contains(4L));
        assertFalse(mastersAfter.get(1L).contains(1L));
        assertEquals(initSet(partitions.get(4L), 1L), mastersAfter.get(4L));

        //Shouldn't change p2 or p3
        assertEquals(partitions.get(2L), mastersAfter.get(2L));
        assertEquals(partitions.get(3L), mastersAfter.get(3L));

        //Should remove a replica of 14L from p1
        assertTrue(replicaPartitions.get(1L).contains(14L));
        assertFalse(replicasAfter.get(1L).contains(14L));

        //Should remove a replica of 1L from p4
        assertTrue(replicaPartitions.get(4L).contains(1L));
        assertFalse(replicasAfter.get(4L).contains(1L));

        //Should add replicas of 4L, 6L, and 10L to p4
        assertFalse(replicaPartitions.get(4L).contains(4L));
        assertFalse(replicaPartitions.get(4L).contains(6L));
        assertFalse(replicaPartitions.get(4L).contains(10L));
        assertTrue(replicasAfter.get(4L).containsAll(initSet(4L, 6L, 10L)));

        //Shouldn't change p2 or p3
        assertEquals(replicaPartitions.get(2L), replicasAfter.get(2L));
        assertEquals(replicaPartitions.get(3L), replicasAfter.get(3L));


        //Large to small test
        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalace(LARGE_TO_SMALL, 2L, 11L); //should move 11L to p1; should remove a replica of 10L from p3
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 11L to p1
        assertTrue(partitions.get(3L).contains(11L));
        assertFalse(partitions.get(1L).contains(11L));
        assertFalse(mastersAfter.get(3L).contains(11L));
        assertTrue(mastersAfter.get(1L).contains(11L));

        //Shouldn't change p2 or p4
        assertEquals(partitions.get(2L), mastersAfter.get(2L));
        assertEquals(partitions.get(4L), mastersAfter.get(4L));

        //Should remove a replica of 10L from p3
        assertTrue(replicaPartitions.get(3L).contains(10L));
        assertFalse(replicasAfter.get(3L).contains(10L));

        //Shouldn't change p1, p2 or p4
        assertEquals(replicaPartitions.get(1L), replicasAfter.get(1L));
        assertEquals(replicaPartitions.get(2L), replicasAfter.get(2L));
        assertEquals(replicaPartitions.get(4L), replicasAfter.get(4L));
    }



    @Test
    public void testUnfriend() {
        int minNumReplicas = 1;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L, 10L));
        partitions.put(3L, initSet(11L, 12L, 13L, 14L, 15L));
        partitions.put(4L, initSet(16L, 17L, 18L, 19L, 20L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L, 15L, 18L));
        friendships.put( 3L, initSet( 4L,  8L, 12L, 16L, 20L));
        friendships.put( 4L, initSet( 5L, 10L, 15L));
        friendships.put( 5L, initSet( 6L, 12L, 18L));
        friendships.put( 6L, initSet( 7L, 14L));
        friendships.put( 7L, initSet( 8L, 16L));
        friendships.put( 8L, initSet( 9L, 18L));
        friendships.put( 9L, initSet(10L, 20L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, initSet(14L));
        friendships.put(14L, initSet(15L));
        friendships.put(15L, initSet(16L));
        friendships.put(16L, initSet(17L));
        friendships.put(17L, initSet(18L));
        friendships.put(18L, initSet(19L));
        friendships.put(19L, initSet(20L));
        friendships.put(20L, Collections.<Long>emptySet());

        Map<Long, Set<Long>> replicaPartitions = new HashMap<Long, Set<Long>>();
        replicaPartitions.put(1L, initSet(16L, 17L, 18L, 19L, 20L));
        replicaPartitions.put(2L, initSet( 1L,  2L,  3L,  4L,  5L, 11L, 18L, 19L));
        replicaPartitions.put(3L, initSet( 6L,  7L,  8L,  9L, 10L,  2L, 17L));
        replicaPartitions.put(4L, initSet(11L, 12L, 13L, 14L, 15L,  3L,  5L));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals((Long) 28L, middleware.getEdgeCut());

        assertEquals(manager.getUserMasterById(10L).getReplicaPartitionIds(), initSet(3L));
        assertEquals(manager.getUserMasterById(11L).getReplicaPartitionIds(), initSet(2L, 4L));

        middleware.unfriend(10L, 11L);

        assertEquals(manager.getUserMasterById(10L).getReplicaPartitionIds(), initSet(3L));
        assertEquals(manager.getUserMasterById(11L).getReplicaPartitionIds(), initSet(4L));


        assertEquals(manager.getUserMasterById( 5L).getReplicaPartitionIds(), initSet(2L, 4L));
        assertEquals(manager.getUserMasterById(18L).getReplicaPartitionIds(), initSet(1L, 2L));

        middleware.unfriend(5L, 18L);

        assertEquals(manager.getUserMasterById( 5L).getReplicaPartitionIds(), initSet(2L));
        assertEquals(manager.getUserMasterById(18L).getReplicaPartitionIds(), initSet(1L, 2L));
    }

    @Test
    public void testRemovePartition() {
        int minNumReplicas = 1;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L, 10L));
        partitions.put(3L, initSet(11L, 12L, 13L, 14L, 15L));
        partitions.put(4L, initSet(16L, 17L, 18L, 19L, 20L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L, 15L, 18L));
        friendships.put( 3L, initSet( 4L,  8L, 12L, 16L, 20L));
        friendships.put( 4L, initSet( 5L, 10L, 15L));
        friendships.put( 5L, initSet( 6L, 12L, 18L));
        friendships.put( 6L, initSet( 7L, 14L));
        friendships.put( 7L, initSet( 8L, 16L));
        friendships.put( 8L, initSet( 9L, 18L));
        friendships.put( 9L, initSet(10L, 20L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, initSet(14L));
        friendships.put(14L, initSet(15L));
        friendships.put(15L, initSet(16L));
        friendships.put(16L, initSet(17L));
        friendships.put(17L, initSet(18L));
        friendships.put(18L, initSet(19L));
        friendships.put(19L, initSet(20L));
        friendships.put(20L, Collections.<Long>emptySet());

        Map<Long, Set<Long>> replicaPartitions = new HashMap<Long, Set<Long>>();
        replicaPartitions.put(1L, initSet(16L, 17L, 18L, 19L, 20L));
        replicaPartitions.put(2L, initSet( 1L,  2L,  3L,  4L,  5L, 19L));
        replicaPartitions.put(3L, initSet( 6L,  7L,  8L,  9L, 10L,  2L, 17L));
        replicaPartitions.put(4L, initSet(11L, 12L, 13L, 14L, 15L,  3L));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals((Long) 28L, middleware.getEdgeCut());

        middleware.removePartition(1L);

        Map<Long, Set<Long>> expectedPMap = new HashMap<Long, Set<Long>>();
        expectedPMap.put(2L, initSet( 6L,  7L,  8L,  9L, 10L,  1L,  4L,  5L));
        expectedPMap.put(3L, initSet(11L, 12L, 13L, 14L, 15L,  2L));
        expectedPMap.put(4L, initSet(16L, 17L, 18L, 19L, 20L,  3L));

        Map<Long, Set<Long>> pMap = middleware.getPartitionToUserMap();

        assertEquals(pMap, expectedPMap);
    }

    @Test
    public void testDetermineUsersWhoWillNeedAnAdditionalReplica() {
        int minNumReplicas = 1;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L, 10L));
        partitions.put(3L, initSet(11L, 12L, 13L, 14L, 15L));
        partitions.put(4L, initSet(16L, 17L, 18L, 19L, 20L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L, 15L, 18L));
        friendships.put( 3L, initSet( 4L,  8L, 12L, 16L, 20L));
        friendships.put( 4L, initSet( 5L, 10L, 15L));
        friendships.put( 5L, initSet( 6L, 12L, 18L));
        friendships.put( 6L, initSet( 7L, 14L));
        friendships.put( 7L, initSet( 8L, 16L));
        friendships.put( 8L, initSet( 9L, 18L));
        friendships.put( 9L, initSet(10L, 20L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, initSet(14L));
        friendships.put(14L, initSet(15L));
        friendships.put(15L, initSet(16L));
        friendships.put(16L, initSet(17L));
        friendships.put(17L, initSet(18L));
        friendships.put(18L, initSet(19L));
        friendships.put(19L, initSet(20L));
        friendships.put(20L, Collections.<Long>emptySet());

        Map<Long, Set<Long>> replicaPartitions = new HashMap<Long, Set<Long>>();
        replicaPartitions.put(1L, initSet(16L, 17L, 18L, 19L, 20L));
        replicaPartitions.put(2L, initSet( 1L,  2L,  3L,  4L,  5L, 19L));
        replicaPartitions.put(3L, initSet( 6L,  7L,  8L,  9L, 10L,  2L, 17L));
        replicaPartitions.put(4L, initSet(11L, 12L, 13L, 14L, 15L,  3L));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals(initSet(1L, 4L, 5L, 16L, 18L, 20L), middleware.determineUsersWhoWillNeedAnAdditionalReplica(1L));
    }

    @Test
    public void testGetRandomPartitionIdWhereThisUserIsNotPresent() {
        int minNumReplicas = 1;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L, 10L));
        partitions.put(3L, initSet(11L, 12L, 13L, 14L, 15L));
        partitions.put(4L, initSet(16L, 17L, 18L, 19L, 20L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L, 15L, 18L));
        friendships.put( 3L, initSet( 4L,  8L, 12L, 16L, 20L));
        friendships.put( 4L, initSet( 5L, 10L, 15L));
        friendships.put( 5L, initSet( 6L, 12L, 18L));
        friendships.put( 6L, initSet( 7L, 14L));
        friendships.put( 7L, initSet( 8L, 16L));
        friendships.put( 8L, initSet( 9L, 18L));
        friendships.put( 9L, initSet(10L, 20L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, initSet(14L));
        friendships.put(14L, initSet(15L));
        friendships.put(15L, initSet(16L));
        friendships.put(16L, initSet(17L));
        friendships.put(17L, initSet(18L));
        friendships.put(18L, initSet(19L));
        friendships.put(19L, initSet(20L));
        friendships.put(20L, Collections.<Long>emptySet());

        Map<Long, Set<Long>> replicaPartitions = new HashMap<Long, Set<Long>>();
        replicaPartitions.put(1L, initSet(16L, 17L, 18L, 19L, 20L));
        replicaPartitions.put(2L, initSet( 1L,  2L,  3L,  4L,  5L));
        replicaPartitions.put(3L, initSet( 6L,  7L,  8L,  9L, 10L));
        replicaPartitions.put(4L, initSet(11L, 12L, 13L, 14L, 15L));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        for(long uid=1L; uid<20L; uid++) {
            long pid = ((uid-1)/5) + 1;
            Set<Long> possibleAnswers = initSet((pid+1L)%4L + 1L, (pid+2L)%4L + 1L);
            long newPid = middleware.getRandomPartitionIdWhereThisUserIsNotPresent(manager.getUserMasterById(uid));
            assertTrue(possibleAnswers.contains(newPid));
        }
    }
}