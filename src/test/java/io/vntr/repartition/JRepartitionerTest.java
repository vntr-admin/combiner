package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
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

        JRepartitioner.State state = initState(1, partitions, friendships);

        Integer partnerId = JRepartitioner.findPartner(6, initSet(1), 2f, state);
        assertTrue(partnerId == 1);

        partnerId = JRepartitioner.findPartner(6, initSet(3, 4), 2f, state);
        assertTrue(partnerId == 3);

        partnerId = JRepartitioner.findPartner(6, initSet(10, 11, 12, 13), 2f, state);
        assertTrue(partnerId == null);

        partnerId = JRepartitioner.findPartner(6, initSet(10, 11, 12, 13), 2.001f, state);
        assertTrue(partnerId == 10);
    }

    @Test
    public void testHowManyFriendsHaveLogicalPartitions() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        friendships.get(1).remove(12);
        friendships.get(12).remove(1);

        JRepartitioner.State state = initState(1, partitions, friendships);

        int[] numFriendsOf1 = JRepartitioner.howManyFriendsHaveLogicalPartitions(1, new int[]{1, 2, 3}, state);
        assertArrayEquals(numFriendsOf1, new int[]{4, 4, 3});

        int[] numFriendsOf2 = JRepartitioner.howManyFriendsHaveLogicalPartitions(2, new int[]{1, 2, 3}, state);
        assertArrayEquals(numFriendsOf2, new int[]{4, 4, 4});
    }

    @Test
    public void testLogicalSwap() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        JRepartitioner.State state = initState(1, partitions, friendships);

        assertTrue(state.getLogicalPids().get(1) == 1);
        assertTrue(state.getLogicalPids().get(2) == 1);
        assertTrue(state.getLogicalPids().get(3) == 1);
        assertTrue(state.getLogicalPids().get(4) == 1);
        assertTrue(state.getLogicalPids().get(5) == 1);
        assertTrue(state.getLogicalPids().get(6) == 2);
        assertTrue(state.getLogicalPids().get(7) == 2);
        assertTrue(state.getLogicalPids().get(8) == 2);
        assertTrue(state.getLogicalPids().get(9) == 2);
        assertTrue(state.getLogicalPids().get(10) == 3);
        assertTrue(state.getLogicalPids().get(11) == 3);
        assertTrue(state.getLogicalPids().get(12) == 3);
        assertTrue(state.getLogicalPids().get(13) == 3);

        JRepartitioner.logicalSwap(1, 6, state);

        assertTrue(state.getLogicalPids().get(1) == 2);
        assertTrue(state.getLogicalPids().get(2) == 1);
        assertTrue(state.getLogicalPids().get(3) == 1);
        assertTrue(state.getLogicalPids().get(4) == 1);
        assertTrue(state.getLogicalPids().get(5) == 1);
        assertTrue(state.getLogicalPids().get(6) == 1);
        assertTrue(state.getLogicalPids().get(7) == 2);
        assertTrue(state.getLogicalPids().get(8) == 2);
        assertTrue(state.getLogicalPids().get(9) == 2);
        assertTrue(state.getLogicalPids().get(10) == 3);
        assertTrue(state.getLogicalPids().get(11) == 3);
        assertTrue(state.getLogicalPids().get(12) == 3);
        assertTrue(state.getLogicalPids().get(13) == 3);
    }

    @Test
    public void testGetEdgeCut() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        JRepartitioner.State state = initState(1, partitions, friendships);

        assertEquals(56, JRepartitioner.getEdgeCut(state.getLogicalPids(), state.getFriendships()));
    }

    @Test
    public void testGetPidsToAssign() {
        TIntSet pids = initSet(1, 2, 4, 5);

        int numUsers = 29;
        int[] results = JRepartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, new int[]{7, 7, 7, 8});

        numUsers = 144;
        results = JRepartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, new int[]{36, 36, 36, 36});

        pids = initSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80);
        numUsers = 100000;
        results = JRepartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, new int[]{1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250});
    }

    private static void assertResultsAreCorrect(int[] results, int numUsers, TIntSet pids, int[] expectedFrequencies) {
        assertTrue(results.length == numUsers);

        TIntIntMap counts = new TIntIntHashMap();
        for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
            counts.put(iter.next(), 0);
        }
        for(int resultPid : results) {
            counts.put(resultPid, counts.get(resultPid)+1);
        }

        assertEquals(counts.keySet(), pids);

        int[] countArray = counts.values();
        Arrays.sort(countArray);
        assertArrayEquals(countArray, expectedFrequencies);
    }

    private static JRepartitioner.State initState(float alpha, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships) {
        JRepartitioner.State state = new JRepartitioner.State(alpha, generateBidirectionalFriendshipSet(friendships));
        state.setLogicalPids(getUToMasterMap(partitions));
        state.initUidToPidToFriendCount(partitions);
        return state;
    }
}
