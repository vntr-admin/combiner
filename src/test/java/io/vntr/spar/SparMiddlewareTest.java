package io.vntr.spar;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.*;

public class SparMiddlewareTest {

    @Test
    public void testBefriend() {
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
        replicaPartitions.put(1L, initSet(16L, 17L, 18L, 19L, 20L, 12L));
        replicaPartitions.put(2L, initSet( 1L,  2L,  3L,  4L,  5L, 11L, 18L, 19L));
        replicaPartitions.put(3L, initSet( 6L,  7L,  8L,  9L, 10L,  2L, 17L));
        replicaPartitions.put(4L, initSet(11L, 12L, 13L, 14L, 15L,  3L,  5L));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals((Long) 28L, middleware.getEdgeCut());

        assertEquals((Long) 1L, manager.getUserMasterById( 3L).getMasterPartitionId());
        assertEquals((Long) 4L, manager.getUserMasterById(18L).getMasterPartitionId());
        assertEquals(initSet(2L, 4L), manager.getUserMasterById( 3L).getReplicaPartitionIds());
        assertEquals(initSet(1L, 2L), manager.getUserMasterById(18L).getReplicaPartitionIds());

        middleware.befriend( 3L, 18L);

        assertEquals((Long) 1L, manager.getUserMasterById( 3L).getMasterPartitionId());
        assertEquals((Long) 4L, manager.getUserMasterById(18L).getMasterPartitionId());
        assertEquals(initSet(2L, 4L), manager.getUserMasterById( 3L).getReplicaPartitionIds());
        assertEquals(initSet(1L, 2L), manager.getUserMasterById(18L).getReplicaPartitionIds());

        assertEquals((Long) 3L, manager.getUserMasterById(12L).getMasterPartitionId());
        assertEquals((Long) 4L, manager.getUserMasterById(18L).getMasterPartitionId());
        assertEquals(initSet(1L, 4L), manager.getUserMasterById(12L).getReplicaPartitionIds());

        middleware.befriend(12L, 18L);

        assertEquals((Long) 4L, manager.getUserMasterById(12L).getMasterPartitionId());
        assertEquals((Long) 4L, manager.getUserMasterById(18L).getMasterPartitionId());
        assertEquals(initSet(1L, 2L), manager.getUserMasterById(18L).getReplicaPartitionIds());
        assertEquals(initSet(1L, 3L), manager.getUserMasterById(12L).getReplicaPartitionIds());
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