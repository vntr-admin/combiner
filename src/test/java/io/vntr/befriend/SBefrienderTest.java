package io.vntr.befriend;

import io.vntr.RepUser;
import io.vntr.spar.SparManager;
import io.vntr.spar.SparTestUtils;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.Utils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static io.vntr.befriend.SBefriender.*;
import static io.vntr.utils.ProbabilityUtils.generateBidirectionalFriendshipSet;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class SBefrienderTest {

    @Test
    public void testDetermineStrategy() {
        for(int i=0; i<10; i++) {
            assertEquals(determineStrategy(4, 4, 4, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(determineStrategy(4, 5, 4, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(determineStrategy(4, 4, 5, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(determineStrategy(4, 5, 5, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
        }

        assertEquals(determineStrategy(5, 4, 5, 2, 3), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 5, 5, 5), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 5, 3, 3), NO_CHANGE);

        assertEquals(determineStrategy(5, 4, 4, 2, 3), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 4, 3, 3), NO_CHANGE);

        assertEquals(determineStrategy(5, 5, 4, 3, 2), SMALL_TO_LARGE);
        assertEquals(determineStrategy(5, 5, 4, 5, 5), SMALL_TO_LARGE);
        assertEquals(determineStrategy(5, 5, 4, 3, 3), NO_CHANGE);
    }

    @Test
    public void testCalcNumReplicasStay() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);

        int uid1 = 7;
        int pid1 = 2;
        RepUser u1 = manager.getUserMasterById(uid1);

        int uid2 = 11;
        int pid2 = 3;
        RepUser u2 = manager.getUserMasterById(uid2);

        int curNumReplicas = replicas.get(pid1).size() + replicas.get(pid2).size();

        int numStay = calcNumReplicasStay(u1, u2, replicas);
        assertTrue(numStay == curNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasMove() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        int uid1 = 7;
        int pid1 = 2;
        RepUser u1 = manager.getUserMasterById(uid1);

        int uid2 = 11;
        int pid2 = 3;
        RepUser u2 = manager.getUserMasterById(uid2);

        int curNumReplicas = replicas.get(pid1).size() + replicas.get(pid2).size();

        int numRelicas1To2 = calcNumReplicasMove(u1, u2, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, bidirectionalFriendships);
        assertTrue(numRelicas1To2 == curNumReplicas + 1);

        int numRelicas2To1 = calcNumReplicasMove(u2, u1, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, bidirectionalFriendships);
        assertTrue(numRelicas2To1 == curNumReplicas);
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        //uid -> pid -> toAdd
        Map<Integer, Map<Integer, Set<Integer>>> expectedResults = new HashMap<>();
        for(int uid : friendships.keySet()) {
            expectedResults.put(uid, new HashMap<Integer, Set<Integer>>());
        }

        expectedResults.get( 1).put(2, initSet(4, 12, 13));
        expectedResults.get( 1).put(3, Collections.<Integer>emptySet());
        expectedResults.get( 2).put(2, initSet(12));
        expectedResults.get( 2).put(3, Collections.<Integer>emptySet());
        expectedResults.get( 3).put(2, initSet(4, 12));
        expectedResults.get( 3).put(3, Collections.<Integer>emptySet());
        expectedResults.get( 4).put(2, Collections.<Integer>emptySet());
        expectedResults.get( 4).put(3, Collections.<Integer>emptySet());
        expectedResults.get( 5).put(2, initSet(4, 12));
        expectedResults.get( 5).put(3, Collections.<Integer>emptySet());
        expectedResults.get( 6).put(1, initSet(11));
        expectedResults.get( 6).put(3, Collections.<Integer>emptySet());
        expectedResults.get( 7).put(1, Collections.<Integer>emptySet());
        expectedResults.get( 7).put(3, Collections.<Integer>emptySet());
        expectedResults.get( 8).put(1, Collections.<Integer>emptySet());
        expectedResults.get( 8).put(3, initSet(7));
        expectedResults.get( 9).put(1, Collections.<Integer>emptySet());
        expectedResults.get( 9).put(3, Collections.<Integer>emptySet());
        expectedResults.get(10).put(1, initSet(11));
        expectedResults.get(10).put(2, initSet(4));
        expectedResults.get(11).put(1, Collections.<Integer>emptySet());
        expectedResults.get(11).put(2, initSet(12));
        expectedResults.get(12).put(1, initSet(11));
        expectedResults.get(12).put(2, initSet(13));
        expectedResults.get(13).put(1, Collections.<Integer>emptySet());
        expectedResults.get(13).put(2, initSet(12));

        for(int uid : friendships.keySet()) {
            for(int pid : partitions.keySet()) {
                if(uidToPidMap.get(uid) == pid) {
                    continue;
                }
                Set<Integer> result = findReplicasToAddToTargetPartition(manager.getUserMasterById(uid), pid, uidToPidMap, uidToReplicasMap);
                assertEquals(result, expectedResults.get(uid).get(pid));
            }
        }
    }

    @Test
    public void testFindReplicasInMovingPartitionToDelete() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        Map<Integer, Map<Integer, Set<Integer>>> expectedResults = new HashMap<>();
        for(int uid : friendships.keySet()) {
            expectedResults.put(uid, new HashMap<Integer, Set<Integer>>());
        }

        //You're deleted if you were only there for the moving user, and numReplicas + (1 if replicate) > minNumReplicas
        expectedResults.get(1).put(2, initSet(13));
        expectedResults.get(1).put(3, Collections.<Integer>emptySet());
        expectedResults.get(2).put(2, initSet(9));
        expectedResults.get(2).put(3, initSet(9));
        expectedResults.get(3).put(2, Collections.<Integer>emptySet());
        expectedResults.get(3).put(3, Collections.<Integer>emptySet());
        expectedResults.get(4).put(2, Collections.<Integer>emptySet());
        expectedResults.get(4).put(3, Collections.<Integer>emptySet());
        expectedResults.get(5).put(2, Collections.<Integer>emptySet());
        expectedResults.get(5).put(3, Collections.<Integer>emptySet());
        expectedResults.get(6).put(1, initSet(5, 11));
        expectedResults.get(6).put(3, initSet(5));
        expectedResults.get(7).put(1, Collections.<Integer>emptySet());
        expectedResults.get(7).put(3, Collections.<Integer>emptySet());
        expectedResults.get(8).put(1, initSet(3));
        expectedResults.get(8).put(3, initSet(3));
        expectedResults.get(9).put(1, initSet(10));
        expectedResults.get(9).put(3, initSet(10));
        expectedResults.get(10).put(1, initSet(9));
        expectedResults.get(10).put(2, initSet(4, 9));
        expectedResults.get(11).put(1, initSet(6));
        expectedResults.get(11).put(2, initSet(6));
        expectedResults.get(12).put(1, initSet(2, 3, 5));
        expectedResults.get(12).put(2, initSet(2, 3, 5));
        expectedResults.get(13).put(1, Collections.<Integer>emptySet());
        expectedResults.get(13).put(2, Collections.<Integer>emptySet());

        for(int uid : friendships.keySet()) {
            for(int pid : partitions.keySet()) {
                if(pid == uidToPidMap.get(uid)) {
                    continue;
                }
                RepUser movingUser = manager.getUserMasterById(uid);
                Set<Integer> replicasToAdd = findReplicasToAddToTargetPartition(movingUser, pid, uidToPidMap, uidToReplicasMap);
                Set<Integer> results = findReplicasInMovingPartitionToDelete(movingUser, replicasToAdd, minNumReplicas, uidToReplicasMap, uidToPidMap, bidirectionalFriendships);
                assertEquals(results, expectedResults.get(uid).get(pid));
            }
        }
    }

    @Test
    public void testFindReplicasInPartitionThatWereOnlyThereForThisUsersSake() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        Map<Integer, Set<Integer>> expectedResults = new HashMap<>();
        expectedResults.put(1, initSet(13));
        expectedResults.put(2, initSet(9));
        expectedResults.put(3, Collections.<Integer>emptySet());
        expectedResults.put(4, Collections.<Integer>emptySet());
        expectedResults.put(5, Collections.<Integer>emptySet());
        expectedResults.put(6, initSet(5, 11));
        expectedResults.put(7, Collections.<Integer>emptySet());
        expectedResults.put(8, initSet(3));
        expectedResults.put(9, initSet(10));
        expectedResults.put(10, initSet(4, 9));
        expectedResults.put(11, initSet(6));
        expectedResults.put(12, initSet(2, 3, 5));
        expectedResults.put(13, Collections.<Integer>emptySet());

        for(int uid : friendships.keySet()) {
            assertEquals(expectedResults.get(uid), findReplicasInPartitionThatWereOnlyThereForThisUsersSake(manager.getUserMasterById(uid), uidToPidMap, bidirectionalFriendships));
        }
    }

    @Test
    public void testShouldWeAddAReplicaOfMovingUserInMovingPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        Boolean[] expectedResults = {null,
                true,  true,  true,  true,  true,
                false, true,  true,  true,  true,
                true,  true,  true };

        for(int uid : friendships.keySet()) {
            RepUser user = manager.getUserMasterById(uid);
            boolean result = shouldWeAddAReplicaOfMovingUserInMovingPartition(user, uidToPidMap);
            assertTrue(result == expectedResults[uid]);
        }
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);


        //Case 1: user has no replica on partition.
        //Should be false.
        assertFalse(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMasterById(13), manager.getUserMasterById(6), minNumReplicas, uidToPidMap));

        //Case 2: user has a replica on partition, and has numReplicas > minNumReplicas.
        //Should be true.
        assertTrue(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMasterById(6), manager.getUserMasterById(13), minNumReplicas, uidToPidMap));

        //Case 3: user has a replica on partition, has numReplicas == minNumReplicas, and is adding one to moving partition.
        //Should be true.
        assertTrue(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMasterById(11), manager.getUserMasterById(6), minNumReplicas, uidToPidMap));

        //Case 4: user has a replica on partition, has numReplicas == minNumReplicas, and is not adding one to moving partition.
        //Should be false.
        manager.unfriend(manager.getUserMasterById(11), manager.getUserMasterById(12));
        manager.unfriend(manager.getUserMasterById(11), manager.getUserMasterById(10));
        assertFalse(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMasterById(11), manager.getUserMasterById(6), minNumReplicas, uidToPidMap));
    }

    @Test
    public void testCouldAndShouldWeDeleteReplicaOfStayingUserInMovingPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        //Case 1: staying user has no replica in moving partition
        //Should: false, Could: false
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMasterById(6), manager.getUserMasterById(13), minNumReplicas, uidToPidMap));
        assertFalse(couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMasterById(6), manager.getUserMasterById(13), uidToPidMap));

        //Case 2: staying user has a replica in moving partition and has another friend there
        //Should: false, Could: false
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMasterById(6), manager.getUserMasterById(1), minNumReplicas, uidToPidMap));
        assertFalse(couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMasterById(6), manager.getUserMasterById(1), uidToPidMap));

        //Case 3: staying user has a replica in moving partition, doesn't have another friend there, and has numReplicas == minNumReplicas
        //Should: false, Could: true
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMasterById(1), manager.getUserMasterById(13), minNumReplicas, uidToPidMap));
        assertTrue (couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMasterById(1), manager.getUserMasterById(13), uidToPidMap));

        //Case 4: staying user has a replica in moving partition, doesn't have another friend there, and has numReplicas > minNumReplicas
        //Should be true
        manager.addReplica(manager.getUserMasterById(13), 2);
        assertTrue (shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMasterById(1), manager.getUserMasterById(13), minNumReplicas, uidToPidMap));
        assertTrue (couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMasterById(1), manager.getUserMasterById(13), uidToPidMap));
    }

}
