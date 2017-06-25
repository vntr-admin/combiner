package io.vntr.befriend;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import org.junit.Test;

import static io.vntr.utils.InitUtils.initRepManager;
import static io.vntr.utils.TroveUtils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static io.vntr.befriend.SBefriender.*;
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
        assertEquals(determineStrategy(5, 4, 5, 6, 5), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 5, 4, 3), NO_CHANGE);

        assertEquals(determineStrategy(5, 4, 4, 2, 3), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 4, 3, 3), NO_CHANGE);

        assertEquals(determineStrategy(5, 5, 4, 3, 2), SMALL_TO_LARGE);
        assertEquals(determineStrategy(5, 5, 4, 5, 6), SMALL_TO_LARGE);
        assertEquals(determineStrategy(5, 5, 4, 3, 4), NO_CHANGE);
    }

    @Test
    public void testCalcNumReplicasStay() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);

        int uid1 = 7;
        int pid1 = 2;
        RepUser u1 = manager.getUserMaster(uid1);

        int uid2 = 11;
        int pid2 = 3;
        RepUser u2 = manager.getUserMaster(uid2);

        int curNumReplicas = replicas.get(pid1).size() + replicas.get(pid2).size();

        int numStay = calcNumReplicasStay(u1, u2, replicas);
        assertTrue(numStay == curNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasMove() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        int uid1 = 7;
        int pid1 = 2;
        RepUser u1 = manager.getUserMaster(uid1);

        int uid2 = 11;
        int pid2 = 3;
        RepUser u2 = manager.getUserMaster(uid2);

        int curNumReplicas = replicas.get(pid1).size() + replicas.get(pid2).size();

        int numReplicas1To2 = calcNumReplicasMove(u1, u2, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, bidirectionalFriendships);
        assertTrue(numReplicas1To2 == curNumReplicas + 1);

        int numReplicas2To1 = calcNumReplicasMove(u2, u1, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, bidirectionalFriendships);
        assertTrue(numReplicas2To1 == curNumReplicas);
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        //uid -> pid -> toAdd
        TIntObjectMap<TIntObjectMap<TIntSet>> expectedResults = new TIntObjectHashMap<>();
        for(int uid : friendships.keys()) {
            expectedResults.put(uid, new TIntObjectHashMap<TIntSet>());
        }

        expectedResults.get( 1).put(2, initSet(4, 12, 13));
        expectedResults.get( 1).put(3, new TIntHashSet());
        expectedResults.get( 2).put(2, initSet(12));
        expectedResults.get( 2).put(3, new TIntHashSet());
        expectedResults.get( 3).put(2, initSet(4, 12));
        expectedResults.get( 3).put(3, new TIntHashSet());
        expectedResults.get( 4).put(2, new TIntHashSet());
        expectedResults.get( 4).put(3, new TIntHashSet());
        expectedResults.get( 5).put(2, initSet(4, 12));
        expectedResults.get( 5).put(3, new TIntHashSet());
        expectedResults.get( 6).put(1, initSet(11));
        expectedResults.get( 6).put(3, new TIntHashSet());
        expectedResults.get( 7).put(1, new TIntHashSet());
        expectedResults.get( 7).put(3, new TIntHashSet());
        expectedResults.get( 8).put(1, new TIntHashSet());
        expectedResults.get( 8).put(3, initSet(7));
        expectedResults.get( 9).put(1, new TIntHashSet());
        expectedResults.get( 9).put(3, new TIntHashSet());
        expectedResults.get(10).put(1, initSet(11));
        expectedResults.get(10).put(2, initSet(4));
        expectedResults.get(11).put(1, new TIntHashSet());
        expectedResults.get(11).put(2, initSet(12));
        expectedResults.get(12).put(1, initSet(11));
        expectedResults.get(12).put(2, initSet(13));
        expectedResults.get(13).put(1, new TIntHashSet());
        expectedResults.get(13).put(2, initSet(12));

        for(int uid : friendships.keys()) {
            for(int pid : partitions.keys()) {
                if(uidToPidMap.get(uid) == pid) {
                    continue;
                }
                TIntSet result = findReplicasToAddToTargetPartition(manager.getUserMaster(uid), pid, uidToPidMap, uidToReplicasMap);
                assertEquals(result, expectedResults.get(uid).get(pid));
            }
        }
    }

    @Test
    public void testFindReplicasInMovingPartitionToDelete() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());
        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        TIntObjectMap<TIntObjectMap<TIntSet>> expectedResults = new TIntObjectHashMap<>();
        for(int uid : friendships.keys()) {
            expectedResults.put(uid, new TIntObjectHashMap<TIntSet>());
        }

        //You're deleted if you were only there for the moving user, and numReplicas + (1 if replicate) > minNumReplicas
        expectedResults.get(1).put(2, initSet(13));
        expectedResults.get(1).put(3, new TIntHashSet());
        expectedResults.get(2).put(2, initSet(9));
        expectedResults.get(2).put(3, initSet(9));
        expectedResults.get(3).put(2, new TIntHashSet());
        expectedResults.get(3).put(3, new TIntHashSet());
        expectedResults.get(4).put(2, new TIntHashSet());
        expectedResults.get(4).put(3, new TIntHashSet());
        expectedResults.get(5).put(2, new TIntHashSet());
        expectedResults.get(5).put(3, new TIntHashSet());
        expectedResults.get(6).put(1, initSet(5, 11));
        expectedResults.get(6).put(3, initSet(5));
        expectedResults.get(7).put(1, new TIntHashSet());
        expectedResults.get(7).put(3, new TIntHashSet());
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
        expectedResults.get(13).put(1, new TIntHashSet());
        expectedResults.get(13).put(2, new TIntHashSet());

        for(int uid : friendships.keys()) {
            for(int pid : partitions.keys()) {
                if(pid == uidToPidMap.get(uid)) {
                    continue;
                }
                RepUser movingUser = manager.getUserMaster(uid);
                TIntSet replicasToAdd = findReplicasToAddToTargetPartition(movingUser, pid, uidToPidMap, uidToReplicasMap);
                TIntSet results = findReplicasInMovingPartitionToDelete(movingUser, replicasToAdd, minNumReplicas, uidToReplicasMap, uidToPidMap, bidirectionalFriendships);
                assertEquals(results, expectedResults.get(uid).get(pid));
            }
        }
    }

    @Test
    public void testFindReplicasInPartitionThatWereOnlyThereForThisUsersSake() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        TIntObjectMap<TIntSet> expectedResults = new TIntObjectHashMap<>();
        expectedResults.put(1, initSet(13));
        expectedResults.put(2, initSet(9));
        expectedResults.put(3, new TIntHashSet());
        expectedResults.put(4, new TIntHashSet());
        expectedResults.put(5, new TIntHashSet());
        expectedResults.put(6, initSet(5, 11));
        expectedResults.put(7, new TIntHashSet());
        expectedResults.put(8, initSet(3));
        expectedResults.put(9, initSet(10));
        expectedResults.put(10, initSet(4, 9));
        expectedResults.put(11, initSet(6));
        expectedResults.put(12, initSet(2, 3, 5));
        expectedResults.put(13, new TIntHashSet());

        for(int uid : friendships.keys()) {
            assertEquals(expectedResults.get(uid), findReplicasInPartitionThatWereOnlyThereForThisUsersSake(manager.getUserMaster(uid), uidToPidMap, bidirectionalFriendships));
        }
    }

    @Test
    public void testShouldWeAddAReplicaOfMovingUserInMovingPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        Boolean[] expectedResults = {null,
                true,  true,  true,  true,  true,
                false, true,  true,  true,  true,
                true,  true,  true };

        for(int uid : friendships.keys()) {
            RepUser user = manager.getUserMaster(uid);
            boolean result = shouldWeAddAReplicaOfMovingUserInMovingPartition(user, uidToPidMap);
            assertTrue(result == expectedResults[uid]);
        }
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);


        //Case 1: user has no replica on partition.
        //Should be false.
        assertFalse(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster(13), manager.getUserMaster(6), minNumReplicas, uidToPidMap));

        //Case 2: user has a replica on partition, and has numReplicas > minNumReplicas.
        //Should be true.
        assertTrue(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster(6), manager.getUserMaster(13), minNumReplicas, uidToPidMap));

        //Case 3: user has a replica on partition, has numReplicas == minNumReplicas, and is adding one to moving partition.
        //Should be true.
        assertTrue(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster(11), manager.getUserMaster(6), minNumReplicas, uidToPidMap));

        //Case 4: user has a replica on partition, has numReplicas == minNumReplicas, and is not adding one to moving partition.
        //Should be false.
        manager.unfriend(manager.getUserMaster(11), manager.getUserMaster(12));
        manager.unfriend(manager.getUserMaster(11), manager.getUserMaster(10));
        assertFalse(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster(11), manager.getUserMaster(6), minNumReplicas, uidToPidMap));
    }

    @Test
    public void testCouldAndShouldWeDeleteReplicaOfStayingUserInMovingPartition() {
        int minNumReplicas = 1;

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
        friendships.put(6,  initSet(11));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(1));

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        //Case 1: staying user has no replica in moving partition
        //Should: false, Could: false
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster(6), manager.getUserMaster(13), minNumReplicas, uidToPidMap));
        assertFalse(couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster(6), manager.getUserMaster(13), uidToPidMap));

        //Case 2: staying user has a replica in moving partition and has another friend there
        //Should: false, Could: false
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster(6), manager.getUserMaster(1), minNumReplicas, uidToPidMap));
        assertFalse(couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster(6), manager.getUserMaster(1), uidToPidMap));

        //Case 3: staying user has a replica in moving partition, doesn't have another friend there, and has numReplicas == minNumReplicas
        //Should: false, Could: true
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster(1), manager.getUserMaster(13), minNumReplicas, uidToPidMap));
        assertTrue (couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster(1), manager.getUserMaster(13), uidToPidMap));

        //Case 4: staying user has a replica in moving partition, doesn't have another friend there, and has numReplicas > minNumReplicas
        //Should be true
        manager.addReplica(manager.getUserMaster(13), 2);
        assertTrue (shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster(1), manager.getUserMaster(13), minNumReplicas, uidToPidMap));
        assertTrue (couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster(1), manager.getUserMaster(13), uidToPidMap));
    }

}
