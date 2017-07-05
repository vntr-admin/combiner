package io.vntr.migration;

import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class SMigratorTest {

    @Test
    public void testGetLeastOverloadedPartition() {
        TIntIntMap pToMasterCounts = new TIntIntHashMap();
        pToMasterCounts.put(1, 10);
        pToMasterCounts.put(2,  7);
        pToMasterCounts.put(3,  9);
        pToMasterCounts.put(4, 11);
        pToMasterCounts.put(5, 13);

        TIntIntMap strategy = new TIntIntHashMap();
        strategy.put(1,  2);
        strategy.put(2,  2);
        strategy.put(3,  2);
        strategy.put(4,  2);
        strategy.put(5,  2);
        strategy.put(6,  2);
        strategy.put(7,  2);
        strategy.put(8,  2);
        strategy.put(9,  2);
        strategy.put(10, 2);
        strategy.put(11, 2);

        strategy.put(12, 3);
        strategy.put(13, 3);
        strategy.put(14, 3);
        strategy.put(15, 3);
        strategy.put(16, 3);
        strategy.put(17, 3);
        strategy.put(18, 3);

        strategy.put(19, 4);
        strategy.put(20, 4);
        strategy.put(21, 4);
        strategy.put(22, 4);

        strategy.put(23,  5);

        int expectedResult = 5;
        int result = SMigrator.getLeastOverloadedPartition(strategy, 1, pToMasterCounts);
        assertTrue(result == expectedResult);
    }

    @Test
    public void testScoreReplicaPromotion() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        TIntObjectMap<TIntFloatMap> expectedResults = new TIntObjectHashMap<>();
        for(int i : friendships.keys()) {
            expectedResults.put(i, new TIntFloatHashMap());
        }

        expectedResults.get( 1).put(2, 0f);
        expectedResults.get( 1).put(3, 0f);
        expectedResults.get( 2).put(2, 0f);
        expectedResults.get( 2).put(3, 0f);
        expectedResults.get( 3).put(2, 0f);
        expectedResults.get( 3).put(3, 0f);
        expectedResults.get( 4).put(2, 0f);
        expectedResults.get( 5).put(2, 0f);
        expectedResults.get( 5).put(3, 0f);
        expectedResults.get( 6).put(1, 0f);
        expectedResults.get( 7).put(1, 0f);
        expectedResults.get( 8).put(1, 0f);
        expectedResults.get( 9).put(1, 0f);
        expectedResults.get( 9).put(3, 0f);
        expectedResults.get(10).put(1, 0f);
        expectedResults.get(10).put(2, 0f);
        expectedResults.get(11).put(2, 0f);
        expectedResults.get(12).put(1, 0f);
        expectedResults.get(13).put(1, 0f);

        //score should be numFriendsOnPartition^2 / numFriendsTotal
        for(int uid : expectedResults.keys()) {
            float numFriends = bidirectionalFriendships.get(uid).size();
            for(int pid : expectedResults.get(uid).keys()) {
                TIntSet friendIds = new TIntHashSet(bidirectionalFriendships.get(uid));
                friendIds.retainAll(partitions.get(pid));
                float friendsOnPartition = friendIds.size();
                expectedResults.get( uid).put(pid, friendsOnPartition * friendsOnPartition / numFriends);
            }
        }

        for(int uid : expectedResults.keys()) {
            for (int pid : expectedResults.get(uid).keys()) {
                float expectedResult = expectedResults.get(uid).get(pid);
                float result = SMigrator.scoreReplicaPromotion(bidirectionalFriendships.get(uid), partitions.get(pid));
                assertTrue(expectedResult == result);
            }
        }
    }

    @Test
    public void testGetRemainingSpotsInPartitions() {
        TIntIntMap pToMasterCounts = new TIntIntHashMap();
        pToMasterCounts.put(1, 10);
        pToMasterCounts.put(2,  7);
        pToMasterCounts.put(3,  9);
        pToMasterCounts.put(4, 11);
        pToMasterCounts.put(5, 13);

        TIntIntMap expectedWithNothingGone = new TIntIntHashMap();
        expectedWithNothingGone.put(1,  0);
        expectedWithNothingGone.put(2,  3);
        expectedWithNothingGone.put(3,  1);
        expectedWithNothingGone.put(4, -1);
        expectedWithNothingGone.put(5, -3);
        assertEquals(expectedWithNothingGone, SMigrator.getRemainingSpotsInPartitions(new TIntHashSet(), 50, pToMasterCounts));

        TIntIntMap expectedWithP1Gone = new TIntIntHashMap();
        expectedWithP1Gone.put(2, 6);
        expectedWithP1Gone.put(3, 4);
        expectedWithP1Gone.put(4, 2);
        expectedWithP1Gone.put(5, 0);
        assertEquals(expectedWithP1Gone, SMigrator.getRemainingSpotsInPartitions(initSet(1), 50, pToMasterCounts));

        TIntIntMap expectedWithP1And2Gone = new TIntIntHashMap();
        expectedWithP1And2Gone.put(3, 8);
        expectedWithP1And2Gone.put(4, 6);
        expectedWithP1And2Gone.put(5, 4);
        assertEquals(expectedWithP1And2Gone, SMigrator.getRemainingSpotsInPartitions(initSet(1, 2), 50, pToMasterCounts));

        TIntIntMap expectedWithP1And2And3Gone = new TIntIntHashMap();
        expectedWithP1And2And3Gone.put(4, 14);
        expectedWithP1And2And3Gone.put(5, 12);
        assertEquals(expectedWithP1And2And3Gone, SMigrator.getRemainingSpotsInPartitions(initSet(1, 2, 3), 50, pToMasterCounts));

        TIntIntMap expectedWithOnly5Remaining = new TIntIntHashMap();
        expectedWithOnly5Remaining.put(5, 37);
        assertEquals(expectedWithOnly5Remaining, SMigrator.getRemainingSpotsInPartitions(initSet(1, 2, 3, 4), 50, pToMasterCounts));
    }

    @Test
    public void testGetNumberOnEachPartition() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntIntMap strategy = new TIntIntHashMap();
        strategy.put(14, 1);
        strategy.put(15, 2);
        strategy.put(16, 3);
        strategy.put(17, 1);
        strategy.put(18, 2);

        TIntIntMap expectedResult = new TIntIntHashMap();
        expectedResult.put(1, 7);
        expectedResult.put(2, 6);
        expectedResult.put(3, 5);

        TIntIntMap result = SMigrator.getNumberOnEachPartition(partitions, strategy);

        assertEquals(expectedResult, result);
    }
}
