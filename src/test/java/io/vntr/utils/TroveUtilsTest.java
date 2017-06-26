package io.vntr.utils;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static io.vntr.utils.TroveUtils.removeUniqueElementFromNonEmptyArray;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/27/17.
 */
public class TroveUtilsTest {

    @Test
    public void testGetUToMasterMap() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));
        partitions.put(4, initSet(21, 22, 23));
        partitions.put(5, initSet(24));
        partitions.put(6, new TIntHashSet());

        TIntIntMap expectedResults = new TIntIntHashMap();
        expectedResults.put( 1, 1);
        expectedResults.put( 2, 1);
        expectedResults.put( 3, 1);
        expectedResults.put( 4, 1);
        expectedResults.put( 5, 1);
        expectedResults.put( 6, 1);
        expectedResults.put( 7, 1);
        expectedResults.put( 8, 2);
        expectedResults.put( 9, 2);
        expectedResults.put(10, 2);
        expectedResults.put(11, 2);
        expectedResults.put(12, 2);
        expectedResults.put(13, 2);
        expectedResults.put(14, 2);
        expectedResults.put(15, 3);
        expectedResults.put(16, 3);
        expectedResults.put(17, 3);
        expectedResults.put(18, 3);
        expectedResults.put(19, 3);
        expectedResults.put(20, 3);
        expectedResults.put(21, 4);
        expectedResults.put(22, 4);
        expectedResults.put(23, 4);
        expectedResults.put(24, 5);

        assertEquals(expectedResults, getUToMasterMap(partitions));
    }

    @Test
    public void testGetUToReplicasMap() {
        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet(21, 22, 23, 8));
        replicas.put(2, initSet(1,  2,  3,  4,  5,  6,  7, 24));
        replicas.put(3, initSet(8,  9, 10, 11, 12, 13, 14));
        replicas.put(4, initSet(1,  2,  3,  4,  5,  6,  7, 15, 16, 17, 18,  19, 20));
        replicas.put(5, initSet(8,  9, 10, 11, 12, 13, 14, 21, 22, 23));
        replicas.put(6, initSet(15, 16, 17, 18,  19, 20, 24));

        TIntObjectMap<TIntSet> expectedResults = new TIntObjectHashMap<>();
        expectedResults.put( 1, initSet(2, 4));
        expectedResults.put( 2, initSet(2, 4));
        expectedResults.put( 3, initSet(2, 4));
        expectedResults.put( 4, initSet(2, 4));
        expectedResults.put( 5, initSet(2, 4));
        expectedResults.put( 6, initSet(2, 4));
        expectedResults.put( 7, initSet(2, 4));
        expectedResults.put( 8, initSet(1, 3, 5));
        expectedResults.put( 9, initSet(3, 5));
        expectedResults.put(10, initSet(3, 5));
        expectedResults.put(11, initSet(3, 5));
        expectedResults.put(12, initSet(3, 5));
        expectedResults.put(13, initSet(3, 5));
        expectedResults.put(14, initSet(3, 5));
        expectedResults.put(15, initSet(4, 6));
        expectedResults.put(16, initSet(4, 6));
        expectedResults.put(17, initSet(4, 6));
        expectedResults.put(18, initSet(4, 6));
        expectedResults.put(19, initSet(4, 6));
        expectedResults.put(20, initSet(4, 6));
        expectedResults.put(21, initSet(1, 5));
        expectedResults.put(22, initSet(1, 5));
        expectedResults.put(23, initSet(1, 5));
        expectedResults.put(24, initSet(2, 6));

        assertEquals(expectedResults, getUToReplicasMap(replicas, expectedResults.keySet()));
    }

    @Test
    public void testGetPToFriendCount() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));

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

        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        TIntObjectMap<TIntIntMap> expectedResults = new TIntObjectHashMap<>();
        for(TIntIterator iter = friendships.keySet().iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            expectedResults.put(uid, new TIntIntHashMap());
        }
        expectedResults.get( 1).put(1, 3);
        expectedResults.get( 1).put(2, 4);
        expectedResults.get( 1).put(3, 3);
        expectedResults.get( 2).put(1, 3);
        expectedResults.get( 2).put(2, 2);
        expectedResults.get( 2).put(3, 2);
        expectedResults.get( 3).put(1, 2);
        expectedResults.get( 3).put(2, 2);
        expectedResults.get( 3).put(3, 2);
        expectedResults.get( 4).put(1, 3);
        expectedResults.get( 4).put(2, 1);
        expectedResults.get( 4).put(3, 1);
        expectedResults.get( 5).put(1, 2);
        expectedResults.get( 5).put(2, 1);
        expectedResults.get( 5).put(3, 1);
        expectedResults.get( 6).put(1, 4);
        expectedResults.get( 6).put(2, 1);
        expectedResults.get( 6).put(3, 0);
        expectedResults.get( 7).put(1, 1);
        expectedResults.get( 7).put(2, 1);
        expectedResults.get( 7).put(3, 1);
        expectedResults.get( 8).put(1, 3);
        expectedResults.get( 8).put(2, 1);
        expectedResults.get( 8).put(3, 1);
        expectedResults.get( 9).put(1, 1);
        expectedResults.get( 9).put(2, 2);
        expectedResults.get( 9).put(3, 1);
        expectedResults.get(10).put(1, 2);
        expectedResults.get(10).put(2, 2);
        expectedResults.get(10).put(3, 0);
        expectedResults.get(11).put(1, 0);
        expectedResults.get(11).put(2, 2);
        expectedResults.get(11).put(3, 0);
        expectedResults.get(12).put(1, 4);
        expectedResults.get(12).put(2, 2);
        expectedResults.get(12).put(3, 0);
        expectedResults.get(13).put(1, 0);
        expectedResults.get(13).put(2, 2);
        expectedResults.get(13).put(3, 0);
        expectedResults.get(14).put(1, 2);
        expectedResults.get(14).put(2, 1);
        expectedResults.get(14).put(3, 1);
        expectedResults.get(15).put(1, 2);
        expectedResults.get(15).put(2, 1);
        expectedResults.get(15).put(3, 1);
        expectedResults.get(16).put(1, 3);
        expectedResults.get(16).put(2, 0);
        expectedResults.get(16).put(3, 2);
        expectedResults.get(17).put(1, 0);
        expectedResults.get(17).put(2, 0);
        expectedResults.get(17).put(3, 2);
        expectedResults.get(18).put(1, 3);
        expectedResults.get(18).put(2, 1);
        expectedResults.get(18).put(3, 2);
        expectedResults.get(19).put(1, 0);
        expectedResults.get(19).put(2, 0);
        expectedResults.get(19).put(3, 2);
        expectedResults.get(20).put(1, 2);
        expectedResults.get(20).put(2, 1);
        expectedResults.get(20).put(3, 1);

        for(TIntIterator iter = expectedResults.keySet().iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            assertEquals(expectedResults.get(uid), getPToFriendCount(uid, bidirectionalFriendships, uidToPidMap, partitions.keySet()));
        }
    }

    @Test
    public void testGetUserCounts() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));
        partitions.put(4, initSet(21, 22, 23));
        partitions.put(5, initSet(24));
        partitions.put(6, new TIntHashSet());

        TIntIntMap expectedResults = new TIntIntHashMap();
        expectedResults.put(1, 7);
        expectedResults.put(2, 7);
        expectedResults.put(3, 6);
        expectedResults.put(4, 3);
        expectedResults.put(5, 1);
        expectedResults.put(6, 0);

        assertEquals(expectedResults, getUserCounts(partitions));
    }

    @Test
    public void testGenerateBidirectionalFriendshipSet() {
        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 17, 20));
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

        TIntObjectMap<TIntSet> expectedBidirectionalFriendships = new TIntObjectHashMap<>();
        expectedBidirectionalFriendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        expectedBidirectionalFriendships.put( 2, initSet( 1,  3,  6,  9, 12, 15, 18));
        expectedBidirectionalFriendships.put( 3, initSet( 2,  4,  8, 12, 16, 20));
        expectedBidirectionalFriendships.put( 4, initSet( 1,  3,  5, 10, 15));
        expectedBidirectionalFriendships.put( 5, initSet( 4,  6, 12, 18));
        expectedBidirectionalFriendships.put( 6, initSet( 1,  2,  5,  7, 14));
        expectedBidirectionalFriendships.put( 7, initSet( 6,  8, 16));
        expectedBidirectionalFriendships.put( 8, initSet( 1,  3,  7,  9, 18));
        expectedBidirectionalFriendships.put( 9, initSet( 2,  8,  10, 17, 20));
        expectedBidirectionalFriendships.put(10, initSet( 1,  4,   9, 11));
        expectedBidirectionalFriendships.put(11, initSet(10, 12));
        expectedBidirectionalFriendships.put(12, initSet( 1,  2,  3,  5,  11,  13));
        expectedBidirectionalFriendships.put(13, initSet(12, 14));
        expectedBidirectionalFriendships.put(14, initSet( 1,  6, 13, 15));
        expectedBidirectionalFriendships.put(15, initSet( 2,  4, 14, 16));
        expectedBidirectionalFriendships.put(16, initSet( 1,  3,  7, 15, 17));
        expectedBidirectionalFriendships.put(17, initSet( 9, 16, 18));
        expectedBidirectionalFriendships.put(18, initSet( 1,  2,  5,  8, 17, 19));
        expectedBidirectionalFriendships.put(19, initSet(18 ,20));
        expectedBidirectionalFriendships.put(20, initSet( 1,  3,  9, 19));

        assertEquals(expectedBidirectionalFriendships, generateBidirectionalFriendshipSet(friendships));
    }

    @Test
    public void testIntersection() {
        TIntSet a = initSet( 1,  2,  3,  4,  5);
        TIntSet b = initSet( 2,  4,  6,  8, 10);
        TIntSet c = initSet( 3,  6,  9, 12, 15);
        TIntSet d = initSet( 7, 11, 13, 17, 19);

        TIntSet expectedAB = initSet(2, 4);
        TIntSet expectedAC = initSet(3);
        TIntSet expectedBC = initSet(6);
        TIntSet expectedAD = new TIntHashSet();
        TIntSet expectedBD = new TIntHashSet();
        TIntSet expectedCD = new TIntHashSet();

        assertEquals(intersection(a, b), expectedAB);
        assertEquals(intersection(a, c), expectedAC);
        assertEquals(intersection(b, c), expectedBC);
        assertEquals(intersection(a, d), expectedAD);
        assertEquals(intersection(b, d), expectedBD);
        assertEquals(intersection(c, d), expectedCD);
        assertEquals(intersection(new TIntHashSet(), new TIntHashSet()), new TIntHashSet());
    }

    @Test
    public void testGetInitialReplicasObeyingKReplication() {
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

        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        int minNumReplicas = 1;
        TIntObjectMap<TIntSet> result = getInitialReplicasObeyingKReplication(minNumReplicas, partitions, bidirectionalFriendships);
        for(int uid : friendships.keys()) {
            int numReplicas = 0;
            for(int pid : partitions.keys()) {
                if(result.get(pid).contains(uid)) {
                    numReplicas++;
                }
            }
            assertTrue(numReplicas >= minNumReplicas);

            for(TIntIterator iter = bidirectionalFriendships.get(uid).iterator(); iter.hasNext(); ) {
                int thisUsersPid = uidToPidMap.get(uid);
                int friendId = iter.next();
                int friendPid = uidToPidMap.get(friendId);
                assertTrue(thisUsersPid == friendPid || result.get(thisUsersPid).contains(friendId));
            }
        }
    }

    @Test
    public void testSingleton() {
        TIntSet expectedResult = new TIntHashSet();
        expectedResult.add(7);
        TIntSet result = singleton(7);
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCopyTIntObjectMapIntSet() {
        TIntObjectMap<TIntSet> expectedResult = new TIntObjectHashMap<>();
        expectedResult.put(1, initSet(1, 2, 3, 4));
        expectedResult.put(2, initSet(2, 4, 6, 8));
        expectedResult.put(3, initSet(3, 6, 9, 12));

        TIntObjectMap<TIntSet> original = new TIntObjectHashMap<>();
        original.put(1, initSet(1, 2, 3, 4));
        original.put(2, initSet(2, 4, 6, 8));
        original.put(3, initSet(3, 6, 9, 12));

        TIntObjectMap<TIntSet> result = copyTIntObjectMapIntSet(original);

        assertEquals(expectedResult, result);
    }

    @Test
    public void testRemoveUniqueElementFromNonEmptyArray() {
        int[] original = new int[]{1, 2, 3, 4, 6, 7, 5};

        int[] expectedResult1 = new int[]{2, 3, 4, 6, 7, 5};
        int[] expectedResult14 = new int[]{2, 3, 6, 7, 5};
        int[] expectedResult146 = new int[]{2, 3, 7, 5};

        int[] result1 = removeUniqueElementFromNonEmptyArray(original, 1);
        assertArrayEquals(expectedResult1, result1);

        int[] result14 = removeUniqueElementFromNonEmptyArray(result1, 4);
        assertArrayEquals(expectedResult14, result14);

        int[] result146 = removeUniqueElementFromNonEmptyArray(result14, 6);
        assertArrayEquals(expectedResult146, result146);

        //double-check that the arrays themselves were not modified
        assertArrayEquals(original, new int[]{1, 2, 3, 4, 6, 7, 5});
        assertArrayEquals(expectedResult1, result1);
        assertArrayEquals(expectedResult14, result14);
        assertArrayEquals(expectedResult146, result146);
    }
}
