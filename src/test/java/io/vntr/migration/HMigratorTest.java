package io.vntr.migration;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.repartition.Target;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class HMigratorTest {

    @Test
    public void testIsOverloaded() {
        TIntIntMap userCounts = new TIntIntHashMap();
        userCounts.put(1, 10);
        userCounts.put(2, 5);
        userCounts.put(3, 6);
        userCounts.put(4, 14);
        userCounts.put(5, 15);
        int numUsers = 0;
        for(int count : userCounts.values()) {
            numUsers += count;
        }

        float differential = 0.0001f;

        //The minus one is because we're dropping a partition
        float averageNumUsers = ((float) numUsers) / (userCounts.size() - 1);

        //The minus one is because we allow it to be overloaded by up to 1 user without considering it overloaded
        float fourteenUserCutoff = ((float) 14-1) / averageNumUsers;
        float fifteenUserCutoff = ((float) 15-1) / averageNumUsers;

        float gamma1 = 1f;
        float gamma2 = fourteenUserCutoff - differential;
        float gamma3 = fourteenUserCutoff + differential;
        float gamma4 = fifteenUserCutoff - differential;
        float gamma5 = fifteenUserCutoff + differential;

        Map<Float, Map<Integer, Boolean>> expectedResults = new HashMap<>();
        expectedResults.put(gamma1, new HashMap<Integer, Boolean>());
        expectedResults.put(gamma2, new HashMap<Integer, Boolean>());
        expectedResults.put(gamma3, new HashMap<Integer, Boolean>());
        expectedResults.put(gamma4, new HashMap<Integer, Boolean>());
        expectedResults.put(gamma5, new HashMap<Integer, Boolean>());

        expectedResults.get(gamma1).put(2, false);
        expectedResults.get(gamma1).put(3, false);
        expectedResults.get(gamma1).put(4, true);
        expectedResults.get(gamma1).put(5, true);

        expectedResults.get(gamma2).put(2, false);
        expectedResults.get(gamma2).put(3, false);
        expectedResults.get(gamma2).put(4, true);
        expectedResults.get(gamma2).put(5, true);

        expectedResults.get(gamma3).put(2, false);
        expectedResults.get(gamma3).put(3, false);
        expectedResults.get(gamma3).put(4, false);
        expectedResults.get(gamma3).put(5, true);

        expectedResults.get(gamma4).put(2, false);
        expectedResults.get(gamma4).put(3, false);
        expectedResults.get(gamma4).put(4, false);
        expectedResults.get(gamma4).put(5, true);

        expectedResults.get(gamma5).put(2, false);
        expectedResults.get(gamma5).put(3, false);
        expectedResults.get(gamma5).put(4, false);
        expectedResults.get(gamma5).put(5, false);

        for(float gamma : expectedResults.keySet()) {
            for(int pid : expectedResults.get(gamma).keySet()) {
                boolean expectedResult = expectedResults.get(gamma).get(pid);
                boolean result = HMigrator.isOverloaded(pid, userCounts, gamma, numUsers);
                assertTrue(expectedResult == result);
            }
        }
    }

    @Test
    public void testGetPreferredTargets() {
        float gamma = 1.05f;

        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12, 13));
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

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        Set<Target> p1Targets = HMigrator.getPreferredTargets(1, uidToPidMap, partitions, bidirectionalFriendships);
        assertTrue(p1Targets.contains(new Target(1, 3, 1, 3f)));
        assertTrue(p1Targets.contains(new Target(2, 2, 1, 2f)));
        assertTrue(p1Targets.contains(new Target(3, 2, 1, 1f)) || p1Targets.contains(new Target(3, 3, 1, 1f)));
        assertTrue(p1Targets.contains(new Target(4, 3, 1, 1f)));
        assertTrue(p1Targets.contains(new Target(5, 2, 1, 1f)) || p1Targets.contains(new Target(5, 3, 1, 1f)));

        Set<Target> p2Targets = HMigrator.getPreferredTargets(2, uidToPidMap, partitions, bidirectionalFriendships);
        assertTrue(p2Targets.contains(new Target(6, 1, 2, 3f)));
        assertTrue(p2Targets.contains(new Target(7, 1, 2, 0f)) || p2Targets.contains(new Target(7, 3, 2, 0f)));
        assertTrue(p2Targets.contains(new Target(8, 1, 2, 2f)));
        assertTrue(p2Targets.contains(new Target(9, 1, 2, 1f)) || p2Targets.contains(new Target(9, 3, 2, 1f)));

        Set<Target> p3Targets = HMigrator.getPreferredTargets(3, uidToPidMap, partitions, bidirectionalFriendships);
        assertTrue(p3Targets.contains(new Target(10, 1, 3, 2f)));
        assertTrue(p3Targets.contains(new Target(11, 1, 3, 0f)) || p3Targets.contains(new Target(11, 2, 3, 0f)));
        assertTrue(p3Targets.contains(new Target(12, 1, 3, 4f)));
        assertTrue(p3Targets.contains(new Target(13, 1, 3, 1f)));
    }

    @Test
    public void testGetPartitionWithFewestUsers() {
        TIntIntMap userCounts = new TIntIntHashMap();
        userCounts.put(1, 10);
        userCounts.put(2, 5);
        userCounts.put(3, 6);
        userCounts.put(4, 14);
        userCounts.put(5, 15);

        assertTrue(HMigrator.getPartitionWithFewestUsers(1, userCounts) == 2);
        assertTrue(HMigrator.getPartitionWithFewestUsers(2, userCounts) == 3);
        assertTrue(HMigrator.getPartitionWithFewestUsers(3, userCounts) == 2);
        assertTrue(HMigrator.getPartitionWithFewestUsers(4, userCounts) == 2);
        assertTrue(HMigrator.getPartitionWithFewestUsers(5, userCounts) == 2);
    }
}
