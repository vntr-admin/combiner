package io.vntr.repartition;

import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.TestUtils;
import org.junit.Test;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class HRepartitionerTest {

    @Test
    public void testInitState() {
        float gamma = 1.05f;

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

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        assertEquals(state.getLogicalPartitions(), partitions);

        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        assertArrayEquals(friendships.keys(), state.getLogicalUsers().keys());
        for(short uid : friendships.keys()) {
            HRepartitioner.LogicalUser user = state.getLogicalUsers().get(uid);

            assertTrue(user.getId() == uid);
            assertTrue(user.getPid() == uidToPidMap.get(uid));
            assertTrue(user.getTotalWeight() == friendships.size());

            TShortShortMap pToWeight = user.getpToWeight();
            for(short pid : partitions.keys()) {
                assertTrue(pToWeight.get(pid) == partitions.get(pid).size());
            }

            TShortShortMap pToFriendCount = user.getpToFriendCount();
            for(short pid : partitions.keys()) {
                TShortSet friends = new TShortHashSet(bidirectionalFriendships.get(uid));
                friends.retainAll(partitions.get(pid));
                assertTrue(pToFriendCount.get(pid) == friends.size());
            }
        }
    }

    @Test
    public void testLogicallyMigrateAndUpdateLogicalUsers() {
        float gamma = 1.05f;

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

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        short uid1 = 1;
        short pid1 = 1;

        short uid2 = 8;
        short pid2 = 2;

        assertTrue(state.getLogicalPartitions().get(pid1).contains(uid1));
        assertTrue(state.getLogicalPartitions().get(pid2).contains(uid2));

        assertEquals(state.getLogicalUsers().get(uid1).getPid(), pid1);
        assertEquals(state.getLogicalUsers().get(uid2).getPid(), pid2);

        Set<Target> targets = TestUtils.initSet(new Target(uid1, pid2, pid1, 1f), new Target(uid2, pid1, pid2, 1f));

        HRepartitioner.logicallyMigrate(targets, state);
        state.updateLogicalUsers(friendships, gamma);

        assertTrue(state.getLogicalPartitions().get(pid1).contains(uid2));
        assertTrue(state.getLogicalPartitions().get(pid2).contains(uid1));

        assertEquals(state.getLogicalUsers().get(uid1).getPid(), pid2);
        assertEquals(state.getLogicalUsers().get(uid2).getPid(), pid1);
    }

    @Test
    public void testGetCandidatesFirstStage() {
        float gamma = 1.5f;

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
        friendships.put((short)6,  initSet(7, 12, 13));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        Set<Target> targets = HRepartitioner.getCandidates((short)2, true, (short)3, state);
        assertEquals(targets, TestUtils.initSet(new Target((short)6, (short)3, (short)2, 1f)));
    }

    @Test
    public void testGetCandidatesSecondStage() {
        float gamma = 1.5f;

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
        friendships.put((short)6,  initSet(7, 12, 13));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        Set<Target> targets = HRepartitioner.getCandidates((short)2, false, (short)3, state);
        assertEquals(targets, TestUtils.initSet(new Target((short)6, (short)1, (short)2, 2f)));
    }
}
