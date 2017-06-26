package io.vntr.migration;

import gnu.trove.map.*;
import gnu.trove.map.hash.*;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.repartition.Target;
import org.junit.Test;

import java.util.Set;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class HMigratorTest {

    @Test
    public void testIsOverloaded() {
        TShortShortMap userCounts = new TShortShortHashMap();
        userCounts.put((short)1, (short)10);
        userCounts.put((short)2, (short)5);
        userCounts.put((short)3, (short)6);
        userCounts.put((short)4, (short)14);
        userCounts.put((short)5, (short)15);
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

        TFloatObjectMap<TShortObjectMap<Boolean>> expectedResults = new TFloatObjectHashMap<>();
        expectedResults.put(gamma1, new TShortObjectHashMap<Boolean>());
        expectedResults.put(gamma2, new TShortObjectHashMap<Boolean>());
        expectedResults.put(gamma3, new TShortObjectHashMap<Boolean>());
        expectedResults.put(gamma4, new TShortObjectHashMap<Boolean>());
        expectedResults.put(gamma5, new TShortObjectHashMap<Boolean>());

        expectedResults.get(gamma1).put((short)2, false);
        expectedResults.get(gamma1).put((short)3, false);
        expectedResults.get(gamma1).put((short)4, true);
        expectedResults.get(gamma1).put((short)5, true);

        expectedResults.get(gamma2).put((short)2, false);
        expectedResults.get(gamma2).put((short)3, false);
        expectedResults.get(gamma2).put((short)4, true);
        expectedResults.get(gamma2).put((short)5, true);

        expectedResults.get(gamma3).put((short)2, false);
        expectedResults.get(gamma3).put((short)3, false);
        expectedResults.get(gamma3).put((short)4, false);
        expectedResults.get(gamma3).put((short)5, true);

        expectedResults.get(gamma4).put((short)2, false);
        expectedResults.get(gamma4).put((short)3, false);
        expectedResults.get(gamma4).put((short)4, false);
        expectedResults.get(gamma4).put((short)5, true);

        expectedResults.get(gamma5).put((short)2, false);
        expectedResults.get(gamma5).put((short)3, false);
        expectedResults.get(gamma5).put((short)4, false);
        expectedResults.get(gamma5).put((short)5, false);

        for(float gamma : expectedResults.keys()) {
            for(short pid : expectedResults.get(gamma).keys()) {
                boolean expectedResult = expectedResults.get(gamma).get(pid);
                boolean result = HMigrator.isOverloaded(pid, userCounts, gamma, numUsers);
                assertTrue(expectedResult == result);
            }
        }
    }

    @Test
    public void testGetPreferredTargets() {
        float gamma = 1.05f;

        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12, 13));
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

        Set<Target> p1Targets = HMigrator.getPreferredTargets((short)1, uidToPidMap, partitions, bidirectionalFriendships);
        assertTrue(p1Targets.contains(new Target((short)1, (short)3, (short)1, 3f)));
        assertTrue(p1Targets.contains(new Target((short)2, (short)2, (short)1, 2f)));
        assertTrue(p1Targets.contains(new Target((short)3, (short)2, (short)1, 1f)) || p1Targets.contains(new Target((short)3, (short)3, (short)1, 1f)));
        assertTrue(p1Targets.contains(new Target((short)4, (short)3, (short)1, 1f)));
        assertTrue(p1Targets.contains(new Target((short)5, (short)2, (short)1, 1f)) || p1Targets.contains(new Target((short)5, (short)3, (short)1, 1f)));

        Set<Target> p2Targets = HMigrator.getPreferredTargets((short)2, uidToPidMap, partitions, bidirectionalFriendships);
        assertTrue(p2Targets.contains(new Target((short)6, (short)1, (short)2, 3f)));
        assertTrue(p2Targets.contains(new Target((short)7, (short)1, (short)2, 0f)) || p2Targets.contains(new Target((short)7, (short)3, (short)2, 0f)));
        assertTrue(p2Targets.contains(new Target((short)8, (short)1, (short)2, 2f)));
        assertTrue(p2Targets.contains(new Target((short)9, (short)1, (short)2, 1f)) || p2Targets.contains(new Target((short)9, (short)3, (short)2, 1f)));

        Set<Target> p3Targets = HMigrator.getPreferredTargets((short)3, uidToPidMap, partitions, bidirectionalFriendships);
        assertTrue(p3Targets.contains(new Target((short)10, (short)1, (short)3, 2f)));
        assertTrue(p3Targets.contains(new Target((short)11, (short)1, (short)3, 0f)) || p3Targets.contains(new Target((short)11, (short)2, (short)3, 0f)));
        assertTrue(p3Targets.contains(new Target((short)12, (short)1, (short)3, 4f)));
        assertTrue(p3Targets.contains(new Target((short)13, (short)1, (short)3, 1f)));
    }

    @Test
    public void testGetPartitionWithFewestUsers() {
        TShortShortMap userCounts = new TShortShortHashMap();
        userCounts.put((short)1, (short)10);
        userCounts.put((short)2, (short)5);
        userCounts.put((short)3, (short)6);
        userCounts.put((short)4, (short)14);
        userCounts.put((short)5, (short)15);

        assertTrue(HMigrator.getPartitionWithFewestUsers((short)1, userCounts) == 2);
        assertTrue(HMigrator.getPartitionWithFewestUsers((short)2, userCounts) == 3);
        assertTrue(HMigrator.getPartitionWithFewestUsers((short)3, userCounts) == 2);
        assertTrue(HMigrator.getPartitionWithFewestUsers((short)4, userCounts) == 2);
        assertTrue(HMigrator.getPartitionWithFewestUsers((short)5, userCounts) == 2);
    }
}
