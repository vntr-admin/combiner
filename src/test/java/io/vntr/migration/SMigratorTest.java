package io.vntr.migration;

import gnu.trove.map.TShortFloatMap;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortFloatHashMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class SMigratorTest {

    @Test
    public void testGetLeastOverloadedPartition() {
        TShortShortMap pToMasterCounts = new TShortShortHashMap();
        pToMasterCounts.put((short)1,(short) 10);
        pToMasterCounts.put((short)2,(short)  7);
        pToMasterCounts.put((short)3,(short)  9);
        pToMasterCounts.put((short)4,(short) 11);
        pToMasterCounts.put((short)5,(short) 13);

        TShortShortMap strategy = new TShortShortHashMap();
        strategy.put((short)1,(short)  2);
        strategy.put((short)2,(short)  2);
        strategy.put((short)3,(short)  2);
        strategy.put((short)4,(short)  2);
        strategy.put((short)5,(short)  2);
        strategy.put((short)6,(short)  2);
        strategy.put((short)7,(short)  2);
        strategy.put((short)8,(short)  2);
        strategy.put((short)9,(short)  2);
        strategy.put((short)10,(short) 2);
        strategy.put((short)11,(short) 2);

        strategy.put((short)12,(short) 3);
        strategy.put((short)13,(short) 3);
        strategy.put((short)14,(short) 3);
        strategy.put((short)15,(short) 3);
        strategy.put((short)16,(short) 3);
        strategy.put((short)17,(short) 3);
        strategy.put((short)18,(short) 3);

        strategy.put((short)19,(short) 4);
        strategy.put((short)20,(short) 4);
        strategy.put((short)21,(short) 4);
        strategy.put((short)22,(short) 4);

        strategy.put((short)23,(short)  5);

        int expectedResult = 5;
        int result = SMigrator.getLeastOverloadedPartition(strategy, (short)1, pToMasterCounts);
        assertTrue(result == expectedResult);
    }

    @Test
    public void testGetLeastOverloadedPartitionWhereThisUserHasAReplica() {
        TShortShortMap pToMasterCounts = new TShortShortHashMap();
        pToMasterCounts.put((short)1,(short) 10);
        pToMasterCounts.put((short)2,(short)  7);
        pToMasterCounts.put((short)3,(short)  9);
        pToMasterCounts.put((short)4,(short) 11);
        pToMasterCounts.put((short)5,(short) 13);

        TShortShortMap strategy = new TShortShortHashMap();
        strategy.put((short)1,(short)  2);
        strategy.put((short)2,(short)  2);
        strategy.put((short)3,(short)  2);
        strategy.put((short)4,(short)  2);
        strategy.put((short)5,(short)  2);
        strategy.put((short)6,(short)  2);
        strategy.put((short)7,(short)  2);
        strategy.put((short)8,(short)  2);
        strategy.put((short)9,(short)  2);
        strategy.put((short)10,(short) 2);
        strategy.put((short)11,(short) 2);

        strategy.put((short)12,(short) 3);
        strategy.put((short)13,(short) 3);
        strategy.put((short)14,(short) 3);
        strategy.put((short)15,(short) 3);
        strategy.put((short)16,(short) 3);
        strategy.put((short)17,(short) 3);
        strategy.put((short)18,(short) 3);

        strategy.put((short)19,(short) 4);
        strategy.put((short)20,(short) 4);
        strategy.put((short)21,(short) 4);
        strategy.put((short)22,(short) 4);

        strategy.put((short)23,(short)  5);

        int expectedResult = 3;
        int result = SMigrator.getLeastOverloadedPartitionWhereThisUserHasAReplica(initSet(2, 3), strategy, pToMasterCounts);
        assertTrue(result == expectedResult);
    }

    @Test
    public void testScoreReplicaPromotion() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put((short)2,  initSet(3, 6, 9, 12));
        friendships.put((short)3,  initSet(4, 8, 12));
        friendships.put((short)4,  initSet(5, 10));
        friendships.put((short)5,  initSet(6, 12));
        friendships.put((short)6,  initSet(7));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  9));

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        TShortObjectMap<TShortFloatMap> expectedResults = new TShortObjectHashMap<>();
        for(short i : friendships.keys()) {
            expectedResults.put(i, new TShortFloatHashMap());
        }

        expectedResults.get((short) 1).put((short) 2, 0f);
        expectedResults.get((short) 1).put((short) 3,  0f);
        expectedResults.get((short) 2).put((short) 2, 0f);
        expectedResults.get((short) 2).put((short) 3,  0f);
        expectedResults.get((short) 3).put((short) 2, 0f);
        expectedResults.get((short) 3).put((short) 3,  0f);
        expectedResults.get((short) 4).put((short) 2, 0f);
        expectedResults.get((short) 5).put((short) 2, 0f);
        expectedResults.get((short) 5).put((short) 3,  0f);
        expectedResults.get((short) 6).put((short) 1,  0f);
        expectedResults.get((short) 7).put((short) 1,  0f);
        expectedResults.get((short) 8).put((short) 1,  0f);
        expectedResults.get((short) 9).put((short) 1,  0f);
        expectedResults.get((short) 9).put((short) 3,  0f);
        expectedResults.get((short)10).put((short) 1,  0f);
        expectedResults.get((short)10).put((short) 2, 0f);
        expectedResults.get((short)11).put((short) 2, 0f);
        expectedResults.get((short)12).put((short) 1,  0f);
        expectedResults.get((short)13).put((short) 1,  0f);

        //score should be numFriendsOnPartition^2 / numFriendsTotal
        for(short uid : expectedResults.keys()) {
            float numFriends = bidirectionalFriendships.get(uid).size();
            for(short pid : expectedResults.get(uid).keys()) {
                TShortSet friendIds = new TShortHashSet(bidirectionalFriendships.get(uid));
                friendIds.retainAll(partitions.get(pid));
                float friendsOnPartition = friendIds.size();
                expectedResults.get( uid).put(pid, friendsOnPartition * friendsOnPartition / numFriends);
            }
        }

        for(short uid : expectedResults.keys()) {
            for (short pid : expectedResults.get(uid).keys()) {
                float expectedResult = expectedResults.get(uid).get(pid);
                float result = SMigrator.scoreReplicaPromotion(bidirectionalFriendships.get(uid), partitions.get(pid));
                assertTrue(expectedResult == result);
            }
        }
    }

    @Test
    public void testGetRemainingSpotsInPartitions() {
        TShortShortMap pToMasterCounts = new TShortShortHashMap();
        pToMasterCounts.put((short)1,(short) 10);
        pToMasterCounts.put((short)2,(short)  7);
        pToMasterCounts.put((short)3,(short)  9);
        pToMasterCounts.put((short)4,(short) 11);
        pToMasterCounts.put((short)5,(short) 13);

        TShortShortMap expectedWithNothingGone = new TShortShortHashMap();
        expectedWithNothingGone.put((short)1,(short)  0);
        expectedWithNothingGone.put((short)2,(short)  3);
        expectedWithNothingGone.put((short)3,(short)  1);
        expectedWithNothingGone.put((short)4, (short)-1);
        expectedWithNothingGone.put((short)5, (short)-3);
        assertEquals(expectedWithNothingGone, SMigrator.getRemainingSpotsInPartitions(new TShortHashSet(), 50, pToMasterCounts));

        TShortShortMap expectedWithP1Gone = new TShortShortHashMap();
        expectedWithP1Gone.put((short)2,(short) 6);
        expectedWithP1Gone.put((short)3,(short) 4);
        expectedWithP1Gone.put((short)4,(short) 2);
        expectedWithP1Gone.put((short)5,(short) 0);
        assertEquals(expectedWithP1Gone, SMigrator.getRemainingSpotsInPartitions(initSet(1), 50, pToMasterCounts));

        TShortShortMap expectedWithP1And2Gone = new TShortShortHashMap();
        expectedWithP1And2Gone.put((short)3,(short) 8);
        expectedWithP1And2Gone.put((short)4,(short) 6);
        expectedWithP1And2Gone.put((short)5,(short) 4);
        assertEquals(expectedWithP1And2Gone, SMigrator.getRemainingSpotsInPartitions(initSet(1, 2), 50, pToMasterCounts));

        TShortShortMap expectedWithP1And2And3Gone = new TShortShortHashMap();
        expectedWithP1And2And3Gone.put((short)4,(short) 14);
        expectedWithP1And2And3Gone.put((short)5,(short) 12);
        assertEquals(expectedWithP1And2And3Gone, SMigrator.getRemainingSpotsInPartitions(initSet(1, 2, 3), 50, pToMasterCounts));

        TShortShortMap expectedWithOnly5Remaining = new TShortShortHashMap();
        expectedWithOnly5Remaining.put((short)5,(short) 37);
        assertEquals(expectedWithOnly5Remaining, SMigrator.getRemainingSpotsInPartitions(initSet(1, 2, 3, 4), 50, pToMasterCounts));
    }

}
