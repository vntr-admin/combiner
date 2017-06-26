package io.vntr.utils;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/27/17.
 */
public class TroveUtilsTest {

    @Test
    public void testGetUToMasterMap() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put((short)2,  initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put((short)3,  initSet(15, 16, 17, 18,  19, 20));
        partitions.put((short)4,  initSet(21, 22, 23));
        partitions.put((short)5,  initSet(24));
        partitions.put((short)6, new TShortHashSet());

        TShortShortMap expectedResults = new TShortShortHashMap();
        expectedResults.put((short) 1, (short) 1);
        expectedResults.put((short) 2, (short) 1);
        expectedResults.put((short) 3, (short) 1);
        expectedResults.put((short) 4, (short) 1);
        expectedResults.put((short) 5, (short) 1);
        expectedResults.put((short) 6, (short) 1);
        expectedResults.put((short) 7, (short) 1);
        expectedResults.put((short) 8, (short) 2);
        expectedResults.put((short) 9, (short) 2);
        expectedResults.put((short)10, (short) 2);
        expectedResults.put((short)11, (short) 2);
        expectedResults.put((short)12, (short) 2);
        expectedResults.put((short)13, (short) 2);
        expectedResults.put((short)14, (short) 2);
        expectedResults.put((short)15, (short) 3);
        expectedResults.put((short)16, (short) 3);
        expectedResults.put((short)17, (short) 3);
        expectedResults.put((short)18, (short) 3);
        expectedResults.put((short)19, (short) 3);
        expectedResults.put((short)20, (short) 3);
        expectedResults.put((short)21, (short) 4);
        expectedResults.put((short)22, (short) 4);
        expectedResults.put((short)23, (short) 4);
        expectedResults.put((short)24, (short) 5);

        assertEquals(expectedResults, getUToMasterMap(partitions));
    }

    @Test
    public void testGetUToReplicasMap() {
        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet(21, 22, 23, 8));
        replicas.put((short)2, initSet(1,  2,  3,  4,  5,  6,  7, 24));
        replicas.put((short)3, initSet(8,  9, 10, 11, 12, 13, 14));
        replicas.put((short)4, initSet(1,  2,  3,  4,  5,  6,  7, 15, 16, 17, 18,  19, 20));
        replicas.put((short)5, initSet(8,  9, 10, 11, 12, 13, 14, 21, 22, 23));
        replicas.put((short)6, initSet(15, 16, 17, 18,  19, 20, 24));

        TShortObjectMap<TShortSet> expectedResults = new TShortObjectHashMap<>();
        expectedResults.put((short) 1, initSet(2, 4));
        expectedResults.put((short) 2, initSet(2, 4));
        expectedResults.put((short) 3, initSet(2, 4));
        expectedResults.put((short) 4, initSet(2, 4));
        expectedResults.put((short) 5, initSet(2, 4));
        expectedResults.put((short) 6, initSet(2, 4));
        expectedResults.put((short) 7, initSet(2, 4));
        expectedResults.put((short) 8, initSet(1, 3, 5));
        expectedResults.put((short) 9, initSet(3, 5));
        expectedResults.put((short)10, initSet(3, 5));
        expectedResults.put((short)11, initSet(3, 5));
        expectedResults.put((short)12, initSet(3, 5));
        expectedResults.put((short)13, initSet(3, 5));
        expectedResults.put((short)14, initSet(3, 5));
        expectedResults.put((short)15, initSet(4, 6));
        expectedResults.put((short)16, initSet(4, 6));
        expectedResults.put((short)17, initSet(4, 6));
        expectedResults.put((short)18, initSet(4, 6));
        expectedResults.put((short)19, initSet(4, 6));
        expectedResults.put((short)20, initSet(4, 6));
        expectedResults.put((short)21, initSet(1, 5));
        expectedResults.put((short)22, initSet(1, 5));
        expectedResults.put((short)23, initSet(1, 5));
        expectedResults.put((short)24, initSet(2, 6));

        assertEquals(expectedResults, getUToReplicasMap(replicas, expectedResults.keySet()));
    }

    @Test
    public void testGetPToFriendCount() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put((short)2,  initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put((short)3,  initSet(15, 16, 17, 18,  19, 20));

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

        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        TShortObjectMap<TShortShortMap> expectedResults = new TShortObjectHashMap<>();
        for(TShortIterator iter = friendships.keySet().iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            expectedResults.put(uid, new TShortShortHashMap());
        }
        expectedResults.get((short) 1).put((short)1, (short) 3);
        expectedResults.get((short) 1).put((short)2, (short) 4);
        expectedResults.get((short) 1).put((short)3, (short) 3);
        expectedResults.get((short) 2).put((short)1, (short) 3);
        expectedResults.get((short) 2).put((short)2, (short) 2);
        expectedResults.get((short) 2).put((short)3, (short) 2);
        expectedResults.get((short) 3).put((short)1, (short) 2);
        expectedResults.get((short) 3).put((short)2, (short) 2);
        expectedResults.get((short) 3).put((short)3, (short) 2);
        expectedResults.get((short) 4).put((short)1, (short) 3);
        expectedResults.get((short) 4).put((short)2, (short) 1);
        expectedResults.get((short) 4).put((short)3, (short) 1);
        expectedResults.get((short) 5).put((short)1, (short) 2);
        expectedResults.get((short) 5).put((short)2, (short) 1);
        expectedResults.get((short) 5).put((short)3, (short) 1);
        expectedResults.get((short) 6).put((short)1, (short) 4);
        expectedResults.get((short) 6).put((short)2, (short) 1);
        expectedResults.get((short) 6).put((short)3, (short) 0);
        expectedResults.get((short) 7).put((short)1, (short) 1);
        expectedResults.get((short) 7).put((short)2, (short) 1);
        expectedResults.get((short) 7).put((short)3, (short) 1);
        expectedResults.get((short) 8).put((short)1, (short) 3);
        expectedResults.get((short) 8).put((short)2, (short) 1);
        expectedResults.get((short) 8).put((short)3, (short) 1);
        expectedResults.get((short) 9).put((short)1, (short) 1);
        expectedResults.get((short) 9).put((short)2, (short) 2);
        expectedResults.get((short) 9).put((short)3, (short) 1);
        expectedResults.get((short)10).put((short)1, (short) 2);
        expectedResults.get((short)10).put((short)2, (short) 2);
        expectedResults.get((short)10).put((short)3, (short) 0);
        expectedResults.get((short)11).put((short)1, (short) 0);
        expectedResults.get((short)11).put((short)2, (short) 2);
        expectedResults.get((short)11).put((short)3, (short) 0);
        expectedResults.get((short)12).put((short)1, (short) 4);
        expectedResults.get((short)12).put((short)2, (short) 2);
        expectedResults.get((short)12).put((short)3, (short) 0);
        expectedResults.get((short)13).put((short)1, (short) 0);
        expectedResults.get((short)13).put((short)2, (short) 2);
        expectedResults.get((short)13).put((short)3, (short) 0);
        expectedResults.get((short)14).put((short)1, (short) 2);
        expectedResults.get((short)14).put((short)2, (short) 1);
        expectedResults.get((short)14).put((short)3, (short) 1);
        expectedResults.get((short)15).put((short)1, (short) 2);
        expectedResults.get((short)15).put((short)2, (short) 1);
        expectedResults.get((short)15).put((short)3, (short) 1);
        expectedResults.get((short)16).put((short)1, (short) 3);
        expectedResults.get((short)16).put((short)2, (short) 0);
        expectedResults.get((short)16).put((short)3, (short) 2);
        expectedResults.get((short)17).put((short)1, (short) 0);
        expectedResults.get((short)17).put((short)2, (short) 0);
        expectedResults.get((short)17).put((short)3, (short) 2);
        expectedResults.get((short)18).put((short)1, (short) 3);
        expectedResults.get((short)18).put((short)2, (short) 1);
        expectedResults.get((short)18).put((short)3, (short) 2);
        expectedResults.get((short)19).put((short)1, (short) 0);
        expectedResults.get((short)19).put((short)2, (short) 0);
        expectedResults.get((short)19).put((short)3, (short) 2);
        expectedResults.get((short)20).put((short)1, (short) 2);
        expectedResults.get((short)20).put((short)2, (short) 1);
        expectedResults.get((short)20).put((short)3, (short) 1);

        for(TShortIterator iter = expectedResults.keySet().iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            assertEquals(expectedResults.get(uid), getPToFriendCount(uid, bidirectionalFriendships, uidToPidMap, partitions.keySet()));
        }
    }

    @Test
    public void testGetUserCounts() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put((short)2,  initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put((short)3,  initSet(15, 16, 17, 18,  19, 20));
        partitions.put((short)4,  initSet(21, 22, 23));
        partitions.put((short)5,  initSet(24));
        partitions.put((short)6, new TShortHashSet());

        TShortShortMap expectedResults = new TShortShortHashMap();
        expectedResults.put((short)1, (short)7);
        expectedResults.put((short)2, (short)7);
        expectedResults.put((short)3, (short)6);
        expectedResults.put((short)4, (short)3);
        expectedResults.put((short)5, (short)1);
        expectedResults.put((short)6, (short)0);

        assertEquals(expectedResults, getUserCounts(partitions));
    }

    @Test
    public void testGenerateBidirectionalFriendshipSet() {
        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put((short)2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put((short)3, initSet( 4,  8, 12, 16, 20));
        friendships.put((short)4, initSet( 5, 10, 15));
        friendships.put((short)5, initSet( 6, 12, 18));
        friendships.put((short)6, initSet( 7, 14));
        friendships.put((short)7, initSet( 8, 16));
        friendships.put((short)8, initSet( 9, 18));
        friendships.put((short)9, initSet(10, 17, 20));
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

        TShortObjectMap<TShortSet> expectedBidirectionalFriendships = new TShortObjectHashMap<>();
        expectedBidirectionalFriendships.put((short)1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        expectedBidirectionalFriendships.put((short)2, initSet( 1,  3,  6,  9, 12, 15, 18));
        expectedBidirectionalFriendships.put((short)3, initSet( 2,  4,  8, 12, 16, 20));
        expectedBidirectionalFriendships.put((short)4, initSet( 1,  3,  5, 10, 15));
        expectedBidirectionalFriendships.put((short)5, initSet( 4,  6, 12, 18));
        expectedBidirectionalFriendships.put((short)6, initSet( 1,  2,  5,  7, 14));
        expectedBidirectionalFriendships.put((short)7, initSet( 6,  8, 16));
        expectedBidirectionalFriendships.put((short)8, initSet( 1,  3,  7,  9, 18));
        expectedBidirectionalFriendships.put((short)9, initSet( 2,  8,  10, 17, 20));
        expectedBidirectionalFriendships.put((short)10, initSet( 1,  4,   9, 11));
        expectedBidirectionalFriendships.put((short)11, initSet(10, 12));
        expectedBidirectionalFriendships.put((short)12, initSet( 1,  2,  3,  5,  11,  13));
        expectedBidirectionalFriendships.put((short)13, initSet(12, 14));
        expectedBidirectionalFriendships.put((short)14, initSet( 1,  6, 13, 15));
        expectedBidirectionalFriendships.put((short)15, initSet( 2,  4, 14, 16));
        expectedBidirectionalFriendships.put((short)16, initSet( 1,  3,  7, 15, 17));
        expectedBidirectionalFriendships.put((short)17, initSet( 9, 16, 18));
        expectedBidirectionalFriendships.put((short)18, initSet( 1,  2,  5,  8, 17, 19));
        expectedBidirectionalFriendships.put((short)19, initSet(18 ,20));
        expectedBidirectionalFriendships.put((short)20, initSet( 1,  3,  9, 19));

        assertEquals(expectedBidirectionalFriendships, generateBidirectionalFriendshipSet(friendships));
    }

    @Test
    public void testIntersection() {
        TShortSet a = initSet( 1,  2,  3,  4,  5);
        TShortSet b = initSet( 2,  4,  6,  8, 10);
        TShortSet c = initSet( 3,  6,  9, 12, 15);
        TShortSet d = initSet( 7, 11, 13, 17, 19);

        TShortSet expectedAB = initSet(2, 4);
        TShortSet expectedAC = initSet(3);
        TShortSet expectedBC = initSet(6);
        TShortSet expectedAD = new TShortHashSet();
        TShortSet expectedBD = new TShortHashSet();
        TShortSet expectedCD = new TShortHashSet();

        assertEquals(intersection(a, b), expectedAB);
        assertEquals(intersection(a, c), expectedAC);
        assertEquals(intersection(b, c), expectedBC);
        assertEquals(intersection(a, d), expectedAD);
        assertEquals(intersection(b, d), expectedBD);
        assertEquals(intersection(c, d), expectedCD);
        assertEquals(intersection(new TShortHashSet(), new TShortHashSet()), new TShortHashSet());
    }

    @Test
    public void testGetInitialReplicasObeyingKReplication() {
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

        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        short minNumReplicas = 1;
        TShortObjectMap<TShortSet> result = getInitialReplicasObeyingKReplication(minNumReplicas, partitions, bidirectionalFriendships);
        for(short uid : friendships.keys()) {
            int numReplicas = 0;
            for(short pid : partitions.keys()) {
                if(result.get(pid).contains(uid)) {
                    numReplicas++;
                }
            }
            assertTrue(numReplicas >= minNumReplicas);

            for(TShortIterator iter = bidirectionalFriendships.get(uid).iterator(); iter.hasNext(); ) {
                short thisUsersPid = uidToPidMap.get(uid);
                short friendId = iter.next();
                short friendPid = uidToPidMap.get(friendId);
                assertTrue(thisUsersPid == friendPid || result.get(thisUsersPid).contains(friendId));
            }
        }
    }

    @Test
    public void testSingleton() {
        TShortSet expectedResult = new TShortHashSet();
        expectedResult.add((short)7);
        TShortSet result = singleton((short)7);
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCopyTIntObjectMapIntSet() {
        TShortObjectMap<TShortSet> expectedResult = new TShortObjectHashMap<>();
        expectedResult.put((short)1, initSet(1, 2, 3, 4));
        expectedResult.put((short)2, initSet(2, 4, 6, 8));
        expectedResult.put((short)3, initSet(3, 6, 9, 12));

        TShortObjectMap<TShortSet> original = new TShortObjectHashMap<>();
        original.put((short)1, initSet(1, 2, 3, 4));
        original.put((short)2, initSet(2, 4, 6, 8));
        original.put((short)3, initSet(3, 6, 9, 12));

        TShortObjectMap<TShortSet> result = copyTShortObjectMapIntSet(original);

        assertEquals(expectedResult, result);
    }

    @Test
    public void testRemoveUniqueElementFromNonEmptyArray() {
        short[] original = new short[]{1, 2, 3, 4, 6, 7, 5};

        short[] expectedResult1 = new short[]{2, 3, 4, 6, 7, 5};
        short[] expectedResult14 = new short[]{2, 3, 6, 7, 5};
        short[] expectedResult146 = new short[]{2, 3, 7, 5};

        short[] result1 = removeUniqueElementFromNonEmptyArray(original, (short)1);
        assertArrayEquals(expectedResult1, result1);

        short[] result14 = removeUniqueElementFromNonEmptyArray(result1, (short)4);
        assertArrayEquals(expectedResult14, result14);

        short[] result146 = removeUniqueElementFromNonEmptyArray(result14, (short)6);
        assertArrayEquals(expectedResult146, result146);

        //double-check that the arrays themselves were not modified
        assertArrayEquals(original, new short[]{1, 2, 3, 4, 6, 7, 5});
        assertArrayEquals(expectedResult1, result1);
        assertArrayEquals(expectedResult14, result14);
        assertArrayEquals(expectedResult146, result146);
    }
}
