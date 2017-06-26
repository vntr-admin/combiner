package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import org.junit.Test;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class JRepartitionerTest {

    @Test
    public void testFindPartner() {
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

        JRepartitioner.State state = initState((short) 1, partitions, friendships);

        Short partnerId = JRepartitioner.findPartner((short) 6, initSet(1), 2f, state);
        assertTrue(partnerId == 1);

        partnerId = JRepartitioner.findPartner((short) 6, initSet(3, 4), 2f, state);
        assertTrue(partnerId == 3);

        partnerId = JRepartitioner.findPartner((short) 6, initSet(10, 11, 12, 13), 2f, state);
        assertTrue(partnerId == null);

        partnerId = JRepartitioner.findPartner((short) 6, initSet(10, 11, 12, 13), 2.001f, state);
        assertTrue(partnerId == 10);
    }

    @Test
    public void testHowManyFriendsHaveLogicalPartitions() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        friendships.get((short) 1).remove((short) 12);
        friendships.get((short) 12).remove((short) 1);

        JRepartitioner.State state = initState((short) 1, partitions, friendships);

        short[] numFriendsOf1 = JRepartitioner.howManyFriendsHaveLogicalPartitions((short) 1, new short[]{1, 2, 3}, state);
        assertArrayEquals(numFriendsOf1, new short[]{4, 4, 3});

        short[] numFriendsOf2 = JRepartitioner.howManyFriendsHaveLogicalPartitions((short) 2, new short[]{1, 2, 3}, state);
        assertArrayEquals(numFriendsOf2, new short[]{4, 4, 4});
    }

    @Test
    public void testLogicalSwap() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        JRepartitioner.State state = initState((short) 1, partitions, friendships);

        assertTrue(state.getLogicalPids().get((short) 1) == 1);
        assertTrue(state.getLogicalPids().get((short) 2) == 1);
        assertTrue(state.getLogicalPids().get((short) 3) == 1);
        assertTrue(state.getLogicalPids().get((short) 4) == 1);
        assertTrue(state.getLogicalPids().get((short) 5) == 1);
        assertTrue(state.getLogicalPids().get((short) 6) == 2);
        assertTrue(state.getLogicalPids().get((short) 7) == 2);
        assertTrue(state.getLogicalPids().get((short) 8) == 2);
        assertTrue(state.getLogicalPids().get((short) 9) == 2);
        assertTrue(state.getLogicalPids().get((short) 10) == 3);
        assertTrue(state.getLogicalPids().get((short) 11) == 3);
        assertTrue(state.getLogicalPids().get((short) 12) == 3);
        assertTrue(state.getLogicalPids().get((short) 13) == 3);

        JRepartitioner.logicalSwap((short) 1, (short)6, state);

        assertTrue(state.getLogicalPids().get((short) 1) == 2);
        assertTrue(state.getLogicalPids().get((short) 2) == 1);
        assertTrue(state.getLogicalPids().get((short) 3) == 1);
        assertTrue(state.getLogicalPids().get((short) 4) == 1);
        assertTrue(state.getLogicalPids().get((short) 5) == 1);
        assertTrue(state.getLogicalPids().get((short) 6) == 1);
        assertTrue(state.getLogicalPids().get((short) 7) == 2);
        assertTrue(state.getLogicalPids().get((short) 8) == 2);
        assertTrue(state.getLogicalPids().get((short) 9) == 2);
        assertTrue(state.getLogicalPids().get((short) 10) == 3);
        assertTrue(state.getLogicalPids().get((short) 11) == 3);
        assertTrue(state.getLogicalPids().get((short) 12) == 3);
        assertTrue(state.getLogicalPids().get((short) 13) == 3);
    }

    @Test
    public void testGetEdgeCut() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        JRepartitioner.State state = initState((short) 1, partitions, friendships);

        assertEquals((short) 56, JRepartitioner.getEdgeCut(state.getLogicalPids(), state.getFriendships()));
    }

    @Test
    public void testGetPidsToAssign() {
        TShortSet pids = initSet(1, 2, 4, 5);

        int numUsers = 29;
        short[] results = JRepartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, new short[]{7, 7, 7, 8});

        numUsers = 144;
        results = JRepartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, new short[]{36, 36, 36, 36});

        pids = initSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80);
        numUsers = 100000;
        results = JRepartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, new short[]{1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250});
    }

    private static void assertResultsAreCorrect(short[] results, int numUsers, TShortSet pids, short[] expectedFrequencies) {
        assertTrue(results.length == numUsers);

        TShortShortMap counts = new TShortShortHashMap();
        for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
            counts.put(iter.next(), (short)0);
        }
        for(short resultPid : results) {
            counts.put(resultPid, (short)(counts.get(resultPid)+1));
        }

        assertEquals(counts.keySet(), pids);

        short[] countArray = counts.values();
        Arrays.sort(countArray);
        assertArrayEquals(countArray, expectedFrequencies);
    }

    private static JRepartitioner.State initState(float alpha, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships) {
        JRepartitioner.State state = new JRepartitioner.State(alpha, generateBidirectionalFriendshipSet(friendships));
        state.setLogicalPids(getUToMasterMap(partitions));
        state.initUidToPidToFriendCount(partitions);
        return state;
    }
}
