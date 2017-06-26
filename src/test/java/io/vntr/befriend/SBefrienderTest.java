package io.vntr.befriend;

import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
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
        for(short i=0; i<10; i++) {
            assertEquals(determineStrategy(4, 4, 4, (short) (Math.random() * 20), (short) (Math.random() * 20)), NO_CHANGE);
            assertEquals(determineStrategy(4, 5, 4, (short) (Math.random() * 20), (short) (Math.random() * 20)), NO_CHANGE);
            assertEquals(determineStrategy(4, 4, 5, (short) (Math.random() * 20), (short) (Math.random() * 20)), NO_CHANGE);
            assertEquals(determineStrategy(4, 5, 5, (short) (Math.random() * 20), (short) (Math.random() * 20)), NO_CHANGE);
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
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(1));

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);

        short uid1 = 7;
        short pid1 = 2;
        RepUser u1 = manager.getUserMaster(uid1);

        short uid2 = 11;
        short pid2 = 3;
        RepUser u2 = manager.getUserMaster(uid2);

        int curNumReplicas = replicas.get(pid1).size() + replicas.get(pid2).size();

        int numStay = calcNumReplicasStay(u1, u2, replicas);
        assertTrue(numStay == curNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasMove() {
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(1));

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        short uid1 = 7;
        short pid1 = 2;
        RepUser u1 = manager.getUserMaster(uid1);

        short uid2 = 11;
        short pid2 = 3;
        RepUser u2 = manager.getUserMaster(uid2);

        int curNumReplicas = replicas.get(pid1).size() + replicas.get(pid2).size();

        int numReplicas1To2 = calcNumReplicasMove(u1, u2, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, bidirectionalFriendships);
        assertTrue(numReplicas1To2 == curNumReplicas + 1);

        int numReplicas2To1 = calcNumReplicasMove(u2, u1, replicas, minNumReplicas, uidToPidMap, uidToReplicasMap, bidirectionalFriendships);
        assertTrue(numReplicas2To1 == curNumReplicas);
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(1));

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        //uid -> pid -> toAdd
        TShortObjectMap<TShortObjectMap<TShortSet>> expectedResults = new TShortObjectHashMap<>();
        for(short uid : friendships.keys()) {
            expectedResults.put(uid, new TShortObjectHashMap<TShortSet>());
        }

        expectedResults.get((short) 1).put((short)2, initSet(4, 12, 13));
        expectedResults.get((short) 1).put((short)3, new TShortHashSet());
        expectedResults.get((short) 2).put((short)2, initSet(12));
        expectedResults.get((short) 2).put((short)3, new TShortHashSet());
        expectedResults.get((short) 3).put((short)2, initSet(4, 12));
        expectedResults.get((short) 3).put((short)3, new TShortHashSet());
        expectedResults.get((short) 4).put((short)2, new TShortHashSet());
        expectedResults.get((short) 4).put((short)3, new TShortHashSet());
        expectedResults.get((short) 5).put((short)2, initSet(4, 12));
        expectedResults.get((short) 5).put((short)3, new TShortHashSet());
        expectedResults.get((short) 6).put((short)1, initSet(11));
        expectedResults.get((short) 6).put((short)3, new TShortHashSet());
        expectedResults.get((short) 7).put((short)1, new TShortHashSet());
        expectedResults.get((short) 7).put((short)3, new TShortHashSet());
        expectedResults.get((short) 8).put((short)1, new TShortHashSet());
        expectedResults.get((short) 8).put((short)3, initSet(7));
        expectedResults.get((short) 9).put((short)1, new TShortHashSet());
        expectedResults.get((short) 9).put((short)3, new TShortHashSet());
        expectedResults.get((short)10).put((short)1, initSet(11));
        expectedResults.get((short)10).put((short)2, initSet(4));
        expectedResults.get((short)11).put((short)1, new TShortHashSet());
        expectedResults.get((short)11).put((short)2, initSet(12));
        expectedResults.get((short)12).put((short)1, initSet(11));
        expectedResults.get((short)12).put((short)2, initSet(13));
        expectedResults.get((short)13).put((short)1, new TShortHashSet());
        expectedResults.get((short)13).put((short)2, initSet(12));

        for(short uid : friendships.keys()) {
            for(short pid : partitions.keys()) {
                if(uidToPidMap.get(uid) == pid) {
                    continue;
                }
                TShortSet result = findReplicasToAddToTargetPartition(manager.getUserMaster(uid), pid, uidToPidMap, uidToReplicasMap);
                assertEquals(result, expectedResults.get(uid).get(pid));
            }
        }
    }

    @Test
    public void testFindReplicasInMovingPartitionToDelete() {
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(1));

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        TShortObjectMap<TShortObjectMap<TShortSet>> expectedResults = new TShortObjectHashMap<>();
        for(short uid : friendships.keys()) {
            expectedResults.put(uid, new TShortObjectHashMap<TShortSet>());
        }

        //You're deleted if you were only there for the moving user, and numReplicas + (1 if replicate) > minNumReplicas
        expectedResults.get((short)1).put((short)2, initSet(13));
        expectedResults.get((short)1).put((short)3, new TShortHashSet());
        expectedResults.get((short)2).put((short)2, initSet(9));
        expectedResults.get((short)2).put((short)3, initSet(9));
        expectedResults.get((short)3).put((short)2, new TShortHashSet());
        expectedResults.get((short)3).put((short)3, new TShortHashSet());
        expectedResults.get((short)4).put((short)2, new TShortHashSet());
        expectedResults.get((short)4).put((short)3, new TShortHashSet());
        expectedResults.get((short)5).put((short)2, new TShortHashSet());
        expectedResults.get((short)5).put((short)3, new TShortHashSet());
        expectedResults.get((short)6).put((short)1, initSet(5, 11));
        expectedResults.get((short)6).put((short)3, initSet(5));
        expectedResults.get((short)7).put((short)1, new TShortHashSet());
        expectedResults.get((short)7).put((short)3, new TShortHashSet());
        expectedResults.get((short)8).put((short)1, initSet(3));
        expectedResults.get((short)8).put((short)3, initSet(3));
        expectedResults.get((short)9).put((short)1, initSet(10));
        expectedResults.get((short)9).put((short)3, initSet(10));
        expectedResults.get((short)10).put((short)1, initSet(9));
        expectedResults.get((short)10).put((short)2, initSet(4, 9));
        expectedResults.get((short)11).put((short)1, initSet(6));
        expectedResults.get((short)11).put((short)2, initSet(6));
        expectedResults.get((short)12).put((short)1, initSet(2, 3, 5));
        expectedResults.get((short)12).put((short)2, initSet(2, 3, 5));
        expectedResults.get((short)13).put((short)1, new TShortHashSet());
        expectedResults.get((short)13).put((short)2, new TShortHashSet());

        for(short uid : friendships.keys()) {
            for(short pid : partitions.keys()) {
                if(pid == uidToPidMap.get(uid)) {
                    continue;
                }
                RepUser movingUser = manager.getUserMaster(uid);
                TShortSet replicasToAdd = findReplicasToAddToTargetPartition(movingUser, pid, uidToPidMap, uidToReplicasMap);
                TShortSet results = findReplicasInMovingPartitionToDelete(movingUser, replicasToAdd, minNumReplicas, uidToReplicasMap, uidToPidMap, bidirectionalFriendships);
                assertEquals(results, expectedResults.get(uid).get(pid));
            }
        }
    }

    @Test
    public void testFindReplicasInPartitionThatWereOnlyThereForThisUsersSake() {
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(1));

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        TShortObjectMap<TShortSet> expectedResults = new TShortObjectHashMap<>();
        expectedResults.put((short)1, initSet(13));
        expectedResults.put((short)2, initSet(9));
        expectedResults.put((short)3, new TShortHashSet());
        expectedResults.put((short)4, new TShortHashSet());
        expectedResults.put((short)5, new TShortHashSet());
        expectedResults.put((short)6, initSet(5, 11));
        expectedResults.put((short)7, new TShortHashSet());
        expectedResults.put((short)8, initSet(3));
        expectedResults.put((short)9, initSet(10));
        expectedResults.put((short)10, initSet(4, 9));
        expectedResults.put((short)11, initSet(6));
        expectedResults.put((short)12, initSet(2, 3, 5));
        expectedResults.put((short)13, new TShortHashSet());

        for(short uid : friendships.keys()) {
            assertEquals(expectedResults.get(uid), findReplicasInPartitionThatWereOnlyThereForThisUsersSake(manager.getUserMaster(uid), uidToPidMap, bidirectionalFriendships));
        }
    }

    @Test
    public void testShouldWeAddAReplicaOfMovingUserInMovingPartition() {
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        Boolean[] expectedResults = {null,
                true,  true,  true,  true,  true,
                false, true,  true,  true,  true,
                true,  true,  true };

        for(short uid : friendships.keys()) {
            RepUser user = manager.getUserMaster(uid);
            boolean result = shouldWeAddAReplicaOfMovingUserInMovingPartition(user, uidToPidMap);
            assertTrue(result == expectedResults[uid]);
        }
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartition() {
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);


        //Case 1: user has no replica on partition.
        //Should be false.
        assertFalse(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster((short)13), manager.getUserMaster((short)6), minNumReplicas, uidToPidMap));

        //Case 2: user has a replica on partition, and has numReplicas > minNumReplicas.
        //Should be true.
        assertTrue(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster((short)6), manager.getUserMaster((short)13), minNumReplicas, uidToPidMap));

        //Case 3: user has a replica on partition, has numReplicas == minNumReplicas, and is adding one to moving partition.
        //Should be true.
        assertTrue(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster((short)11), manager.getUserMaster((short)6), minNumReplicas, uidToPidMap));

        //Case 4: user has a replica on partition, has numReplicas == minNumReplicas, and is not adding one to moving partition.
        //Should be false.
        manager.unfriend(manager.getUserMaster((short)11), manager.getUserMaster((short)12));
        manager.unfriend(manager.getUserMaster((short)11), manager.getUserMaster((short)10));
        assertFalse(shouldWeDeleteReplicaOfMovingUserInStayingPartition(manager.getUserMaster((short)11), manager.getUserMaster((short)6), minNumReplicas, uidToPidMap));
    }

    @Test
    public void testCouldAndShouldWeDeleteReplicaOfStayingUserInMovingPartition() {
        short minNumReplicas = 1;

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
        friendships.put((short)6,  initSet(11));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(1));

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        RepManager manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        //Case 1: staying user has no replica in moving partition
        //Should: false, Could: false
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster((short)6), manager.getUserMaster((short)13), minNumReplicas, uidToPidMap));
        assertFalse(couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster((short)6), manager.getUserMaster((short)13), uidToPidMap));

        //Case 2: staying user has a replica in moving partition and has another friend there
        //Should: false, Could: false
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster((short)6), manager.getUserMaster((short)1), minNumReplicas, uidToPidMap));
        assertFalse(couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster((short)6), manager.getUserMaster((short)1), uidToPidMap));

        //Case 3: staying user has a replica in moving partition, doesn't have another friend there, and has numReplicas == minNumReplicas
        //Should: false, Could: true
        assertFalse(shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster((short)1), manager.getUserMaster((short)13), minNumReplicas, uidToPidMap));
        assertTrue (couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster((short)1), manager.getUserMaster((short)13), uidToPidMap));

        //Case 4: staying user has a replica in moving partition, doesn't have another friend there, and has numReplicas > minNumReplicas
        //Should be true
        manager.addReplica(manager.getUserMaster((short)13), (short)2);
        assertTrue (shouldWeDeleteReplicaOfStayingUserInMovingPartition(manager.getUserMaster((short)1), manager.getUserMaster((short)13), minNumReplicas, uidToPidMap));
        assertTrue (couldWeDeleteReplicaOfStayingUserInMovingPartition (manager.getUserMaster((short)1), manager.getUserMaster((short)13), uidToPidMap));
    }

}
