package io.vntr.befriend;

import io.vntr.repartition.HRepartitioner;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.Utils.generateBidirectionalFriendshipSet;
import static io.vntr.utils.Utils.getUToMasterMap;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 5/3/17.
 */
public class HBefriender2Test {

    @Test
    public void testCalculateGain() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
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
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        float differential = 0.0001f;
        float averageWeight = ((float) friendships.size()) / ((float) partitions.size());
        float gammaBarelyAllowingSixOnPartition = (6f / averageWeight) + differential;
        float gammaNearlyAllowingSixOnPartition = (6f / averageWeight) - differential;
        float gammaBarelyAllowingFiveOnPartition = (5f / averageWeight) + differential;
        float gammaNearlyAllowingFiveOnPartition = (5f / averageWeight) - differential;

        Map<Integer, HRepartitioner.LogicalUser> lusers = HRepartitioner.initLogicalUsers(partitions, bidirectionalFriendships, friendships.keySet(), gammaBarelyAllowingSixOnPartition);

        Map<Integer, Map<Integer, Integer>> expectedResults = new HashMap<>();
        for(int uid : friendships.keySet()) {
            expectedResults.put(uid, new HashMap<Integer, Integer>());
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

        for(int uid : friendships.keySet()) {
            int usersPid = uidToPidMap.get(uid);
            for(int pid : partitions.keySet()) {
                if(pid != usersPid) {
                    int result = HBefriender2.calculateGain(lusers.get(uid), pid, gammaBarelyAllowingSixOnPartition);
                    int expectedResult = expectedResults.get(uid).get(pid);
                    assertTrue(result == expectedResult);
                }
            }
        }

        assertTrue( 2 == HBefriender2.calculateGain(lusers.get(12), 1, gammaBarelyAllowingSixOnPartition));
        assertTrue(-1 == HBefriender2.calculateGain(lusers.get(12), 1, gammaNearlyAllowingSixOnPartition));

        assertTrue( 0 == HBefriender2.calculateGain(lusers.get(1), 3, gammaBarelyAllowingFiveOnPartition));
        assertTrue(-1 == HBefriender2.calculateGain(lusers.get(1), 3, gammaNearlyAllowingFiveOnPartition));
    }

    @Test
    public void testDetermineBestBefriendingRebalanceStrategy() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put(2,  initSet(3, 6, 9, 12));
        friendships.put(3,  initSet(4, 8, 12));
        friendships.put(4,  initSet(5, 10));
        friendships.put(5,  initSet(6, 12));
        friendships.put(6,  initSet(7));
        friendships.put(7,  initSet(8, 10));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        float differential = 0.0001f;
        float averageWeight = ((float) friendships.size()) / ((float) partitions.size());
        float gammaBarelyAllowingSixOnPartition = (6f / averageWeight) + differential;
        float gammaNearlyAllowingSixOnPartition = (6f / averageWeight) - differential;
        float gammaBarelyAllowingFiveOnPartition = (5f / averageWeight) + differential;
        float gammaNearlyAllowingFiveOnPartition = (5f / averageWeight) - differential;

        BEFRIEND_REBALANCE_STRATEGY result = HBefriender2.determineBestBefriendingRebalanceStrategy(1, 6, gammaBarelyAllowingSixOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL);

        result = HBefriender2.determineBestBefriendingRebalanceStrategy(1, 6, gammaNearlyAllowingSixOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE);

        result = HBefriender2.determineBestBefriendingRebalanceStrategy(10, 6, gammaBarelyAllowingFiveOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE);

        result = HBefriender2.determineBestBefriendingRebalanceStrategy(10, 6, gammaNearlyAllowingFiveOnPartition, bidirectionalFriendships, partitions);
        assertTrue(result == BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE);
    }

}
