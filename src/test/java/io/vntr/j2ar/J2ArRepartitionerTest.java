package io.vntr.j2ar;

import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.ProbabilityUtils.generateBidirectionalFriendshipSet;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/18/17.
 */
public class J2ArRepartitionerTest {
    @Test
    public void testFindPartner() {
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

        J2ArRepartitioner.State state = initState(1, 2, 0.5f, 5, partitions, friendships);

        Integer partnerId = J2ArRepartitioner.findPartner(6, initSet(1), 2f, state);
        assertTrue(partnerId == 1);

        partnerId = J2ArRepartitioner.findPartner(6, initSet(3, 4), 2f, state);
        assertTrue(partnerId == 3);

        partnerId = J2ArRepartitioner.findPartner(6, initSet(10, 11, 12, 13), 2f, state);
        assertTrue(partnerId == null);

        partnerId = J2ArRepartitioner.findPartner(6, initSet(10, 11, 12, 13), 2.001f, state);
        assertTrue(partnerId == 10);
    }

    @Test
    public void testHowManyFriendsHaveLogicalPartition() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        friendships.get(1).remove(12);
        friendships.get(12).remove(1);

        J2ArRepartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);

        int[] numFriendsOf1 = J2ArRepartitioner.howManyFriendsHaveLogicalPartitions(1, new int[]{1, 2, 3}, state);
        assertArrayEquals(numFriendsOf1, new int[]{4, 4, 3});

        int[] numFriendsOf2 = J2ArRepartitioner.howManyFriendsHaveLogicalPartitions(2, new int[]{1, 2, 3}, state);
        assertArrayEquals(numFriendsOf2, new int[]{4, 4, 4});

    }

    @Test
    public void testLogicalSwap() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        J2ArRepartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);

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

        J2ArRepartitioner.logicalSwap(1, 6, state);

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
    public void testPhysicallyMigrate() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        J2ArRepartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);
        J2ArManager manager = J2ArInitUtils.initGraph(1, 1, 1, 1, 0, partitions, friendships);

        J2ArRepartitioner.logicalSwap(1, 10, state);
        J2ArRepartitioner.logicalSwap(5, 7, state);

        J2ArRepartitioner repartitioner = new J2ArRepartitioner(manager, 1, 1, 1, 1);

        assertEquals(partitions, manager.getPartitionToUsers());

        repartitioner.physicallyMigrate(state.getLogicalPids());

        Map<Integer, Set<Integer>> newPartitions = new HashMap<>();
        newPartitions.put(1, initSet( 10,  2,  3,  4, 7));
        newPartitions.put(2, initSet( 6,  5,  8,  9));
        newPartitions.put(3, initSet(1, 11, 12, 13));

        assertEquals(newPartitions, manager.getPartitionToUsers());
    }

    @Test
    public void testGetLogicalEdgeCut() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        J2ArRepartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);

        assertEquals(56, J2ArRepartitioner.getLogicalEdgeCut(state));
    }

    private static J2ArRepartitioner.State initState(float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Integer> inversePartitions = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            for(Integer uid : partitions.get(pid)) {
                inversePartitions.put(uid, pid);
            }
        }

        J2ArRepartitioner.State state = new J2ArRepartitioner.State(alpha, initialT, deltaT, k, generateBidirectionalFriendshipSet(friendships));
        state.setLogicalPids(inversePartitions);
        return state;
    }


}
