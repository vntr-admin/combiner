package io.vntr.middleware;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
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
        short minNumReplicas = 1;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4,  5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9, 10));
        partitions.put((short)3,  initSet(11, 12, 13, 14, 15));
        partitions.put((short)4,  initSet(16, 17, 18, 19, 20));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put((short)2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put((short)3, initSet( 4,  8, 12, 16, 20));
        friendships.put((short)4, initSet( 5, 10, 15));
        friendships.put((short)5, initSet( 6, 12, 18));
        friendships.put((short)6, initSet( 7, 14));
        friendships.put((short)7, initSet( 8, 16));
        friendships.put((short)8, initSet( 9, 18));
        friendships.put((short)9, initSet(10, 20));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(14));
        friendships.put((short)14, initSet(15));
        friendships.put((short)15, initSet(16));
        friendships.put((short)16, initSet(17));
        friendships.put((short)17, initSet(18));
        friendships.put((short)18, initSet(19));
        friendships.put((short)19, initSet(20));
        friendships.put((short)20, new TShortHashSet());

        TShortObjectMap<TShortSet> replicaPartitions = new TShortObjectHashMap<>();
        replicaPartitions.put((short)1,  initSet( 6,  8,  9, 10, 12, 14, 15, 16, 17, 18, 19, 20));
        replicaPartitions.put((short)2,  initSet( 1,  2,  3,  4,  5, 11, 14, 16, 18, 19, 20));
        replicaPartitions.put((short)3,  initSet( 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 16, 17));
        replicaPartitions.put((short)4,  initSet( 1,  2,  3,  5,  7,  8,  9, 11, 12, 13, 14, 15));

        //No change test 1: no replicas added
        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        middleware.performRebalance(NO_CHANGE,  (short)3, (short)19); //should not change anything, including replicas
        TShortObjectMap<TShortSet> mastersAfter   = manager.getPartitionToUserMap();
        TShortObjectMap<TShortSet> replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should not change replicas
        assertEquals(replicaPartitions, replicasAfter);


        //No change test 2: one replica added
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(NO_CHANGE,  (short)7, (short)17); //should add 17 replica to p2
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17 to p2 and change nothing else
        assertFalse(replicaPartitions.get((short)2).contains((short)17));
        assertEquals(new TShortHashSet(initSet(replicaPartitions.get((short)2), 17)), replicasAfter.get((short)2));

        //Should change nothing else
        assertEquals(new TShortHashSet(replicaPartitions.get((short)1)), replicasAfter.get((short)1));
        assertEquals(new TShortHashSet(replicaPartitions.get((short)3)), replicasAfter.get((short)3));
        assertEquals(new TShortHashSet(replicaPartitions.get((short)4)), replicasAfter.get((short)4));


        //No change test 3: 2 replicas added
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(NO_CHANGE, (short)10, (short)17); //should add 17 replica to p2 and a copy of 10 to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should not change masters
        assertEquals(partitions, mastersAfter);

        //Should add 17 to p2
        assertFalse(replicaPartitions.get((short)2).contains((short)17));
        assertEquals(new TShortHashSet(initSet(replicaPartitions.get((short)2), 17)), replicasAfter.get((short)2));

        //Should add 10 to p4
        assertFalse(replicaPartitions.get((short)4).contains((short)10));
        assertEquals(new TShortHashSet(initSet(replicaPartitions.get((short)4), 10)), replicasAfter.get((short)4));

        //Should change nothing else
        assertEquals(new TShortHashSet(replicaPartitions.get((short)1)), replicasAfter.get((short)1));
        assertEquals(new TShortHashSet(replicaPartitions.get((short)3)), replicasAfter.get((short)3));

        //Small to large test
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(SMALL_TO_LARGE, (short)1, (short)19); //should move 1 to p4; should remove a replica of 14 from p1; should remove a replica of 1 from p4; should add replicas of 4, 6, and 10 to p4
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 1 to p4
        assertTrue(partitions.get((short)1).contains((short)4));
        assertFalse(partitions.get((short)4).contains((short)4));
        assertFalse(mastersAfter.get((short)1).contains((short)1));
        assertEquals(new TShortHashSet(initSet(partitions.get((short)4), 1)), mastersAfter.get((short)4));

        //Shouldn't change p2 or p3
        assertEquals(new TShortHashSet(partitions.get((short)2)), mastersAfter.get((short)2));
        assertEquals(new TShortHashSet(partitions.get((short)3)), mastersAfter.get((short)3));

        //Should remove a replica of 14 from p1
        assertTrue(replicaPartitions.get((short)1).contains((short)14));
        assertFalse(replicasAfter.get((short)1).contains((short)14));

        //Should remove a replica of 1 from p4
        assertTrue(replicaPartitions.get((short)4).contains((short)1));
        assertFalse(replicasAfter.get((short)4).contains((short)1));

        //Should add replicas of 4, 6, and 10 to p4
        assertFalse(replicaPartitions.get((short)4).contains((short)4));
        assertFalse(replicaPartitions.get((short)4).contains((short)6));
        assertFalse(replicaPartitions.get((short)4).contains((short)10));
        assertTrue(replicasAfter.get((short)4).containsAll(initSet(4, 6, 10)));

        //Shouldn't change p2 or p3
        assertEquals(new TShortHashSet(replicaPartitions.get((short)2)), replicasAfter.get((short)2));
        assertEquals(new TShortHashSet(replicaPartitions.get((short)3)), replicasAfter.get((short)3));


        //Large to small test
        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        middleware = new SparMiddleware(manager);

        middleware.performRebalance(LARGE_TO_SMALL, (short)2, (short)11); //should move 11 to p1; should remove a replica of 10 from p3
        mastersAfter   = manager.getPartitionToUserMap();
        replicasAfter  = manager.getPartitionToReplicasMap();

        //Should move 11 to p1
        assertTrue(partitions.get((short)3).contains((short)11));
        assertFalse(partitions.get((short)1).contains((short)11));
        assertFalse(mastersAfter.get((short)3).contains((short)11));
        assertTrue(mastersAfter.get((short)1).contains((short)11));

        //Shouldn't change p2 or p4
        assertEquals(new TShortHashSet(partitions.get((short)2)), mastersAfter.get((short)2));
        assertEquals(new TShortHashSet(partitions.get((short)4)), mastersAfter.get((short)4));

        //Should remove a replica of 10 from p3
        assertTrue(replicaPartitions.get((short)3).contains((short)10));
        assertFalse(replicasAfter.get((short)3).contains((short)10));

        //Shouldn't change p1, p2 or p4
        assertEquals(new TShortHashSet(replicaPartitions.get((short)1)), replicasAfter.get((short)1));
        assertEquals(new TShortHashSet(replicaPartitions.get((short)2)), replicasAfter.get((short)2));
        assertEquals(new TShortHashSet(replicaPartitions.get((short)4)), replicasAfter.get((short)4));
    }



    @Test
    public void testUnfriend() {
        short minNumReplicas = 1;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, TroveUtils.initSet( 1,  2,  3,  4,  5));
        partitions.put((short)2, TroveUtils.initSet( 6,  7,  8,  9, 10));
        partitions.put((short)3, TroveUtils.initSet(11, 12, 13, 14, 15));
        partitions.put((short)4, TroveUtils.initSet(16, 17, 18, 19, 20));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short) 1, TroveUtils.initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put((short) 2, TroveUtils.initSet( 3,  6,  9, 12, 15, 18));
        friendships.put((short) 3, TroveUtils.initSet( 4,  8, 12, 16, 20));
        friendships.put((short) 4, TroveUtils.initSet( 5, 10, 15));
        friendships.put((short) 5, TroveUtils.initSet( 6, 12, 18));
        friendships.put((short) 6, TroveUtils.initSet( 7, 14));
        friendships.put((short) 7, TroveUtils.initSet( 8, 16));
        friendships.put((short) 8, TroveUtils.initSet( 9, 18));
        friendships.put((short) 9, TroveUtils.initSet(10, 20));
        friendships.put((short)10, TroveUtils.initSet(11));
        friendships.put((short)11, TroveUtils.initSet(12));
        friendships.put((short)12, TroveUtils.initSet(13));
        friendships.put((short)13, TroveUtils.initSet(14));
        friendships.put((short)14, TroveUtils.initSet(15));
        friendships.put((short)15, TroveUtils.initSet(16));
        friendships.put((short)16, TroveUtils.initSet(17));
        friendships.put((short)17, TroveUtils.initSet(18));
        friendships.put((short)18, TroveUtils.initSet(19));
        friendships.put((short)19, TroveUtils.initSet(20));
        friendships.put((short)20, new TShortHashSet());

        TShortObjectMap<TShortSet> replicaPartitions = new TShortObjectHashMap<>();
        replicaPartitions.put((short)1, TroveUtils.initSet(16, 17, 18, 19, 20));
        replicaPartitions.put((short)2, TroveUtils.initSet( 1,  2,  3,  4,  5, 11, 18, 19));
        replicaPartitions.put((short)3, TroveUtils.initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put((short)4, TroveUtils.initSet(11, 12, 13, 14, 15,  3,  5));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertTrue(28 == middleware.getEdgeCut());

        assertEquals(manager.getUserMaster((short)10).getReplicaPids(), TroveUtils.initSet(3));
        assertEquals(manager.getUserMaster((short)11).getReplicaPids(), TroveUtils.initSet(2, 4));

        middleware.unfriend((short)10, (short)11);

        assertEquals(manager.getUserMaster((short)10).getReplicaPids(), TroveUtils.initSet(3));
        assertEquals(manager.getUserMaster((short)11).getReplicaPids(), TroveUtils.initSet(4));


        assertEquals(manager.getUserMaster((short) 5).getReplicaPids(), TroveUtils.initSet(2, 4));
        assertEquals(manager.getUserMaster((short)18).getReplicaPids(), TroveUtils.initSet(1, 2));

        middleware.unfriend((short)5, (short)18);

        assertEquals(manager.getUserMaster((short) 5).getReplicaPids(), TroveUtils.initSet(2));
        assertEquals(manager.getUserMaster((short)18).getReplicaPids(), TroveUtils.initSet(1, 2));
    }

    @Test
    public void testRemovePartition() {
        short minNumReplicas = 1;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4,  5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9, 10));
        partitions.put((short)3,  initSet(11, 12, 13, 14, 15));
        partitions.put((short)4,  initSet(16, 17, 18, 19, 20));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put((short)2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put((short)3, initSet( 4,  8, 12, 16, 20));
        friendships.put((short)4, initSet( 5, 10, 15));
        friendships.put((short)5, initSet( 6, 12, 18));
        friendships.put((short)6, initSet( 7, 14));
        friendships.put((short)7, initSet( 8, 16));
        friendships.put((short)8, initSet( 9, 18));
        friendships.put((short)9, initSet(10, 20));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(14));
        friendships.put((short)14, initSet(15));
        friendships.put((short)15, initSet(16));
        friendships.put((short)16, initSet(17));
        friendships.put((short)17, initSet(18));
        friendships.put((short)18, initSet(19));
        friendships.put((short)19, initSet(20));
        friendships.put((short)20, new TShortHashSet());

        TShortObjectMap<TShortSet> replicaPartitions = new TShortObjectHashMap<>();
        replicaPartitions.put((short)1,  initSet(16, 17, 18, 19, 20));
        replicaPartitions.put((short)2,  initSet( 1,  2,  3,  4,  5, 19));
        replicaPartitions.put((short)3,  initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put((short)4,  initSet(11, 12, 13, 14, 15,  3));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertTrue(28 == middleware.getEdgeCut());

        middleware.removePartition((short)1);

        TShortObjectMap<TShortSet> expectedPMap = new TShortObjectHashMap<>();
        expectedPMap.put((short)2, initSet( 6,  7,  8,  9, 10,  1,  4,  5));
        expectedPMap.put((short)3, initSet(11, 12, 13, 14, 15,  2));
        expectedPMap.put((short)4, initSet(16, 17, 18, 19, 20,  3));

        TShortObjectMap<TShortSet> pMap = middleware.getPartitionToUserMap();

        assertEquals(pMap, expectedPMap);
    }

    @Test
    public void testDetermineUsersWhoWillNeedAnAdditionalReplica() {
        short minNumReplicas = 1;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4,  5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9, 10));
        partitions.put((short)3,  initSet(11, 12, 13, 14, 15));
        partitions.put((short)4,  initSet(16, 17, 18, 19, 20));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put((short)2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put((short)3, initSet( 4,  8, 12, 16, 20));
        friendships.put((short)4, initSet( 5, 10, 15));
        friendships.put((short)5, initSet( 6, 12, 18));
        friendships.put((short)6, initSet( 7, 14));
        friendships.put((short)7, initSet( 8, 16));
        friendships.put((short)8, initSet( 9, 18));
        friendships.put((short)9, initSet(10, 20));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(14));
        friendships.put((short)14, initSet(15));
        friendships.put((short)15, initSet(16));
        friendships.put((short)16, initSet(17));
        friendships.put((short)17, initSet(18));
        friendships.put((short)18, initSet(19));
        friendships.put((short)19, initSet(20));
        friendships.put((short)20, new TShortHashSet());

        TShortObjectMap<TShortSet> replicaPartitions = new TShortObjectHashMap<>();
        replicaPartitions.put((short)1,  initSet(16, 17, 18, 19, 20));
        replicaPartitions.put((short)2,  initSet( 1,  2,  3,  4,  5, 19));
        replicaPartitions.put((short)3,  initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put((short)4,  initSet(11, 12, 13, 14, 15,  3));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        assertEquals(TroveUtils.initSet(1, 4, 5, 16, 18, 20), middleware.determineUsersWhoWillNeedAnAdditionalReplica((short)1));
    }

    @Test
    public void testGetRandomPidWhereThisUserIsNotPresent() {
        short minNumReplicas = 1;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4,  5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9, 10));
        partitions.put((short)3,  initSet(11, 12, 13, 14, 15));
        partitions.put((short)4,  initSet(16, 17, 18, 19, 20));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put((short)2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put((short)3, initSet( 4,  8, 12, 16, 20));
        friendships.put((short)4, initSet( 5, 10, 15));
        friendships.put((short)5, initSet( 6, 12, 18));
        friendships.put((short)6, initSet( 7, 14));
        friendships.put((short)7, initSet( 8, 16));
        friendships.put((short)8, initSet( 9, 18));
        friendships.put((short)9, initSet(10, 20));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(14));
        friendships.put((short)14, initSet(15));
        friendships.put((short)15, initSet(16));
        friendships.put((short)16, initSet(17));
        friendships.put((short)17, initSet(18));
        friendships.put((short)18, initSet(19));
        friendships.put((short)19, initSet(20));
        friendships.put((short)20, new TShortHashSet());

        TShortObjectMap<TShortSet> replicaPartitions = new TShortObjectHashMap<>();
        replicaPartitions.put((short)1,  initSet(16, 17, 18, 19, 20));
        replicaPartitions.put((short)2,  initSet( 1,  2,  3,  4,  5));
        replicaPartitions.put((short)3,  initSet( 6,  7,  8,  9, 10));
        replicaPartitions.put((short)4,  initSet(11, 12, 13, 14, 15));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);
        SparMiddleware middleware = new SparMiddleware(manager);

        for(short uid=1; uid<20; uid++) {
            short pid = (short)(((uid-1)/5) + 1);
            TShortSet possibleAnswers = initSet((pid+1)%4 + 1, (pid+2)%4 + 1);
            short newPid = middleware.getRandomPidWhereThisUserIsNotPresent(manager.getUserMaster(uid), new TShortHashSet());
            assertTrue(possibleAnswers.contains(newPid));
        }
    }
}