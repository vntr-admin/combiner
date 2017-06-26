package io.vntr.befriend;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.repartition.HRepartitioner;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 5/3/17.
 */
public class HBefrienderTest {

    @Test
    public void testCalculateGain() {
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

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        float differential = 0.0001f;
        float averageWeight = ((float) friendships.size()) / ((float) partitions.size());
        float gammaBarelyAllowingSixOnPartition = (6f / averageWeight) + differential;
        float gammaNearlyAllowingSixOnPartition = (6f / averageWeight) - differential;
        float gammaBarelyAllowingFiveOnPartition = (5f / averageWeight) + differential;
        float gammaNearlyAllowingFiveOnPartition = (5f / averageWeight) - differential;

        TShortObjectMap<HRepartitioner.LogicalUser> lusers = HRepartitioner.initLogicalUsers(partitions, bidirectionalFriendships, new TShortHashSet(friendships.keys()), gammaBarelyAllowingSixOnPartition);

        TIntObjectMap<TIntIntMap> expectedResults = new TIntObjectHashMap<>();
        for(short uid : friendships.keys()) {
            expectedResults.put(uid, new TIntIntHashMap());
        }

        expectedResults.get(1) .put(2, 0);
        expectedResults.get(1) .put(3, 0);
        expectedResults.get(2) .put(2, 0);
        expectedResults.get(2) .put(3, -1);
        expectedResults.get(3) .put(2, -1);
        expectedResults.get(3) .put(3, -1);
        expectedResults.get(4) .put(2, -3);
        expectedResults.get(4) .put(3, -2);
        expectedResults.get(5) .put(2, 0);
        expectedResults.get(5) .put(3, 0);
        expectedResults.get(6) .put(1, 2);
        expectedResults.get(6) .put(3, -1);
        expectedResults.get(7) .put(1, -2);
        expectedResults.get(7) .put(3, -2);
        expectedResults.get(8) .put(1, 0);
        expectedResults.get(8) .put(3, -2);
        expectedResults.get(9) .put(1, 0);
        expectedResults.get(9) .put(3, 0);
        expectedResults.get(10).put(1, 1);
        expectedResults.get(10).put(2, 0);
        expectedResults.get(11).put(1, -2);
        expectedResults.get(11).put(2, -2);
        expectedResults.get(12).put(1, 2);
        expectedResults.get(12).put(2, -2);
        expectedResults.get(13).put(1, -1);
        expectedResults.get(13).put(2, -1);

        for(short uid : friendships.keys()) {
            int usersPid = uidToPidMap.get(uid);
            for(short pid : partitions.keys()) {
                if(pid != usersPid) {
                    int result = HBefriender.calculateGain(lusers.get(uid), pid, gammaBarelyAllowingSixOnPartition);
                    int expectedResult = expectedResults.get(uid).get(pid);
                    assertTrue(result == expectedResult);
                }
            }
        }

        assertTrue( 2 == HBefriender.calculateGain(lusers.get((short)12), (short)1, gammaBarelyAllowingSixOnPartition));
        assertTrue(-1 == HBefriender.calculateGain(lusers.get((short)12), (short)1, gammaNearlyAllowingSixOnPartition));

        assertTrue( 0 == HBefriender.calculateGain(lusers.get((short)1), (short)3, gammaBarelyAllowingFiveOnPartition));
        assertTrue(-1 == HBefriender.calculateGain(lusers.get((short)1), (short)3, gammaNearlyAllowingFiveOnPartition));
    }

    @Test
    public void testDetermineBestBefriendingRebalanceStrategy() {
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
        friendships.put((short)7,  initSet(8, 10));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        float differential = 0.0001f;
        float averageWeight = ((float) friendships.size()) / ((float) partitions.size());
        float gammaBarelyAllowingSixOnPartition = (6f / averageWeight) + differential;
        float gammaNearlyAllowingSixOnPartition = (6f / averageWeight) - differential;
        float gammaBarelyAllowingFiveOnPartition = (5f / averageWeight) + differential;
        float gammaNearlyAllowingFiveOnPartition = (5f / averageWeight) - differential;

        BEFRIEND_REBALANCE_STRATEGY result = HBefriender.determineBestBefriendingRebalanceStrategy((short)1, (short)6, gammaBarelyAllowingSixOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL);

        result = HBefriender.determineBestBefriendingRebalanceStrategy((short)1, (short)6, gammaNearlyAllowingSixOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE);

        result = HBefriender.determineBestBefriendingRebalanceStrategy((short)10, (short)6, gammaBarelyAllowingFiveOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE);

        result = HBefriender.determineBestBefriendingRebalanceStrategy((short)10, (short)6, gammaNearlyAllowingFiveOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE);
    }

}
