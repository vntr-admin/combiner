package io.vntr.middleware;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.manager.RepManager;
import io.vntr.utils.TroveUtils;
import org.junit.Test;

import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static io.vntr.utils.InitUtils.initRepManager;
import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.*;

public class SparMiddlewareTest {

    @Test
    public void testPerformRebalance() {
        int minNumReplicas = 1;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, initSet( 6,  8,  9, 10, 12, 14, 15, 16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 11, 14, 16, 18, 19, 20));
        replicaPartitions.put(3, initSet( 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 16, 17));
        replicaPartitions.put(4, initSet( 1,  2,  3,  5,  7,  8,  9, 11, 12, 13, 14, 15));

        //No change test 1: no replicas added
        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        middleware.performRebalance(NO_CHANGE,  3, 19); //should not change anything, including replicas
        TIntObjectMap<TIntSet> mastersAfter   = manager.getPartitionToUserMap();
        TIntObjectMap<TIntSet> replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should not change replicas
        assertEquals(replicaPartitions, replicasAfter);


        //No change test 2: one replica added
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(NO_CHANGE,  7, 17); //should add 17 replica to p2
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17 to p2 and change nothing else
        assertFalse(replicaPartitions.get(2).contains(17));
        assertEquals(new TIntHashSet(initSet(replicaPartitions.get(2), 17)), replicasAfter.get(2));

        //Should change nothing else
        assertEquals(new TIntHashSet(replicaPartitions.get(1)), replicasAfter.get(1));
        assertEquals(new TIntHashSet(replicaPartitions.get(3)), replicasAfter.get(3));
        assertEquals(new TIntHashSet(replicaPartitions.get(4)), replicasAfter.get(4));


        //No change test 3: 2 replicas added
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(NO_CHANGE, 10, 17); //should add 17 replica to p2 and a copy of 10 to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17 to p2
        assertFalse(replicaPartitions.get(2).contains(17));
        assertEquals(new TIntHashSet(initSet(replicaPartitions.get(2), 17)), replicasAfter.get(2));

        //Should add 10 to p4
        assertFalse(replicaPartitions.get(4).contains(10));
        assertEquals(new TIntHashSet(initSet(replicaPartitions.get(4), 10)), replicasAfter.get(4));

        //Should change nothing else
        assertEquals(new TIntHashSet(replicaPartitions.get(1)), replicasAfter.get(1));
        assertEquals(new TIntHashSet(replicaPartitions.get(3)), replicasAfter.get(3));

        //Small to large test
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(SMALL_TO_LARGE, 1, 19); //should move 1 to p4; should remove a replica of 14 from p1; should remove a replica of 1 from p4; should add replicas of 4, 6, and 10 to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 1 to p4
        assertTrue(partitions.get(1).contains(4));
        assertFalse(partitions.get(4).contains(4));
        assertFalse(mastersAfter.get(1).contains(1));
        assertEquals(new TIntHashSet(initSet(partitions.get(4), 1)), mastersAfter.get(4));

        //Shouldn't change p2 or p3
        assertEquals(new TIntHashSet(partitions.get(2)), mastersAfter.get(2));
        assertEquals(new TIntHashSet(partitions.get(3)), mastersAfter.get(3));

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
        assertEquals(new TIntHashSet(replicaPartitions.get(2)), replicasAfter.get(2));
        assertEquals(new TIntHashSet(replicaPartitions.get(3)), replicasAfter.get(3));


        //Large to small test
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(LARGE_TO_SMALL, 2, 11); //should move 11 to p1; should remove a replica of 10 from p3
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 11 to p1
        assertTrue(partitions.get(3).contains(11));
        assertFalse(partitions.get(1).contains(11));
        assertFalse(mastersAfter.get(3).contains(11));
        assertTrue(mastersAfter.get(1).contains(11));

        //Shouldn't change p2 or p4
        assertEquals(new TIntHashSet(partitions.get(2)), mastersAfter.get(2));
        assertEquals(new TIntHashSet(partitions.get(4)), mastersAfter.get(4));

        //Should remove a replica of 10 from p3
        assertTrue(replicaPartitions.get(3).contains(10));
        assertFalse(replicasAfter.get(3).contains(10));

        //Shouldn't change p1, p2 or p4
        assertEquals(new TIntHashSet(replicaPartitions.get(1)), replicasAfter.get(1));
        assertEquals(new TIntHashSet(replicaPartitions.get(2)), replicasAfter.get(2));
        assertEquals(new TIntHashSet(replicaPartitions.get(4)), replicasAfter.get(4));
    }



    @Test
    public void testUnfriend() {
        int minNumReplicas = 1;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4,  5));
        partitions.put(2, TroveUtils.initSet( 6,  7,  8,  9, 10));
        partitions.put(3, TroveUtils.initSet(11, 12, 13, 14, 15));
        partitions.put(4, TroveUtils.initSet(16, 17, 18, 19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put( 1, TroveUtils.initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, TroveUtils.initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, TroveUtils.initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, TroveUtils.initSet( 5, 10, 15));
        friendships.put( 5, TroveUtils.initSet( 6, 12, 18));
        friendships.put( 6, TroveUtils.initSet( 7, 14));
        friendships.put( 7, TroveUtils.initSet( 8, 16));
        friendships.put( 8, TroveUtils.initSet( 9, 18));
        friendships.put( 9, TroveUtils.initSet(10, 20));
        friendships.put(10, TroveUtils.initSet(11));
        friendships.put(11, TroveUtils.initSet(12));
        friendships.put(12, TroveUtils.initSet(13));
        friendships.put(13, TroveUtils.initSet(14));
        friendships.put(14, TroveUtils.initSet(15));
        friendships.put(15, TroveUtils.initSet(16));
        friendships.put(16, TroveUtils.initSet(17));
        friendships.put(17, TroveUtils.initSet(18));
        friendships.put(18, TroveUtils.initSet(19));
        friendships.put(19, TroveUtils.initSet(20));
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, TroveUtils.initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, TroveUtils.initSet( 1,  2,  3,  4,  5, 11, 18, 19));
        replicaPartitions.put(3, TroveUtils.initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put(4, TroveUtils.initSet(11, 12, 13, 14, 15,  3,  5));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals((Integer) 28, middleware.getEdgeCut());

        assertEquals(manager.getUserMaster(10).getReplicaPids(), TroveUtils.initSet(3));
        assertEquals(manager.getUserMaster(11).getReplicaPids(), TroveUtils.initSet(2, 4));

        middleware.unfriend(10, 11);

        assertEquals(manager.getUserMaster(10).getReplicaPids(), TroveUtils.initSet(3));
        assertEquals(manager.getUserMaster(11).getReplicaPids(), TroveUtils.initSet(4));


        assertEquals(manager.getUserMaster( 5).getReplicaPids(), TroveUtils.initSet(2, 4));
        assertEquals(manager.getUserMaster(18).getReplicaPids(), TroveUtils.initSet(1, 2));

        middleware.unfriend(5, 18);

        assertEquals(manager.getUserMaster( 5).getReplicaPids(), TroveUtils.initSet(2));
        assertEquals(manager.getUserMaster(18).getReplicaPids(), TroveUtils.initSet(1, 2));
    }

    @Test
    public void testRemovePartition() {
        int minNumReplicas = 1;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 19));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15,  3));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals((Integer) 28, middleware.getEdgeCut());

        middleware.removePartition(1);

        TIntObjectMap<TIntSet> expectedPMap = new TIntObjectHashMap<>();
        expectedPMap.put(2, initSet( 6,  7,  8,  9, 10,  1,  4,  5));
        expectedPMap.put(3, initSet(11, 12, 13, 14, 15,  2));
        expectedPMap.put(4, initSet(16, 17, 18, 19, 20,  3));

        TIntObjectMap<TIntSet> pMap = middleware.getPartitionToUserMap();

        assertEquals(pMap, expectedPMap);
    }

    @Test
    public void testDetermineUsersWhoWillNeedAnAdditionalReplica() {
        int minNumReplicas = 1;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 19));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15,  3));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals(TroveUtils.initSet(1, 4, 5, 16, 18, 20), middleware.determineUsersWhoWillNeedAnAdditionalReplica(1));
    }

    @Test
    public void testGetRandomPidWhereThisUserIsNotPresent() {
        int minNumReplicas = 1;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        for(int uid=1; uid<20; uid++) {
            int pid = ((uid-1)/5) + 1;
            TIntSet possibleAnswers = initSet((pid+1)%4 + 1, (pid+2)%4 + 1);
            int newPid = middleware.getRandomPidWhereThisUserIsNotPresent(manager.getUserMaster(uid), new TIntHashSet());
            assertTrue(possibleAnswers.contains(newPid));
        }
    }
}