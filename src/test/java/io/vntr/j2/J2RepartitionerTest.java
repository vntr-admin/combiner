package io.vntr.j2;

import io.vntr.utils.ProbabilityUtils;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class J2RepartitionerTest {

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

        J2Repartitioner.State state = initState(1, 2, 0.5f, 5, partitions, friendships);

        Integer partnerId = J2Repartitioner.findPartner(6, initSet(1), 2f, state);
        assertTrue(partnerId == 1);

        partnerId = J2Repartitioner.findPartner(6, initSet(3, 4), 2f, state);
        assertTrue(partnerId == 3);

        partnerId = J2Repartitioner.findPartner(6, initSet(10, 11, 12, 13), 2f, state);
        assertTrue(partnerId == null);

        partnerId = J2Repartitioner.findPartner(6, initSet(10, 11, 12, 13), 2.001f, state);
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

        J2Repartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);

        int numFriendsOf1On1 = J2Repartitioner.howManyFriendsHaveLogicalPartition(1, 1, state);
        assertTrue(numFriendsOf1On1 == 4);

        int numFriendsOf1On2 = J2Repartitioner.howManyFriendsHaveLogicalPartition(1, 2, state);
        assertTrue(numFriendsOf1On2 == 4);

        int numFriendsOf1On3 = J2Repartitioner.howManyFriendsHaveLogicalPartition(1, 3, state);
        assertTrue(numFriendsOf1On3 == 3);


        int numFriendsOf2On1 = J2Repartitioner.howManyFriendsHaveLogicalPartition(2, 1, state);
        assertTrue(numFriendsOf2On1 == 4);

        int numFriendsOf2On2 = J2Repartitioner.howManyFriendsHaveLogicalPartition(2, 2, state);
        assertTrue(numFriendsOf2On2 == 4);

        int numFriendsOf2On3 = J2Repartitioner.howManyFriendsHaveLogicalPartition(2, 3, state);
        assertTrue(numFriendsOf2On3 == 4);
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

        J2Repartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);

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

        J2Repartitioner.logicalSwap(1, 6, state);

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

        J2Repartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);
        J2Manager manager = J2InitUtils.initGraph(1, 1, 1, 1, 0, partitions, friendships);

        J2Repartitioner.logicalSwap(1, 10, state);
        J2Repartitioner.logicalSwap(5, 7, state);

        J2Repartitioner repartitioner = new J2Repartitioner(manager, 1, 1, 1, 1);

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

        J2Repartitioner.State state = initState(1, 1, 1, 1, partitions, friendships);

        assertEquals(56, J2Repartitioner.getLogicalEdgeCut(state));
    }

    @Test
    public void testGetPidsToAssign() {
        Set<Integer> pids = initSet(1, 2, 4, 5);

        int numUsers = 29;
        Integer[] results = J2Repartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, Arrays.asList(7, 7, 7, 8));

        numUsers = 144;
        results = J2Repartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, Arrays.asList(36, 36, 36, 36));

        pids = initSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80);
        numUsers = 100000;
        results = J2Repartitioner.getPidsToAssign(numUsers, pids);
        assertResultsAreCorrect(results, numUsers, pids, Arrays.asList(1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250, 1250));
    }

    private static void assertResultsAreCorrect(Integer[] results, int numUsers, Set<Integer> pids, List<Integer> expectedFrequencies) {
        assertTrue(results.length == numUsers);

        Map<Integer, Integer> counts = new HashMap<>();
        for(int pid : pids) {
            counts.put(pid, 0);
        }
        for(int resultPid : results) {
            counts.put(resultPid, counts.get(resultPid)+1);
        }

        assertEquals(counts.keySet(), pids);

        List<Integer> countList = new LinkedList<>(counts.values());
        Collections.sort(countList);
        assertEquals(countList, expectedFrequencies);
    }

    private static J2Repartitioner.State initState(float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Integer> inversePartitions = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            for(Integer uid : partitions.get(pid)) {
                inversePartitions.put(uid, pid);
            }
        }

        J2Repartitioner.State state = new J2Repartitioner.State(alpha, initialT, deltaT, k, ProbabilityUtils.generateBidirectionalFriendshipSet(friendships));
        state.setLogicalPids(inversePartitions);
        return state;
    }

}
