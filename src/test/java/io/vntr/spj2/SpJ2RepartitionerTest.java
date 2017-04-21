package io.vntr.spj2;

import io.vntr.RepUser;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.spj2.SpJ2Repartitioner.*;
import static io.vntr.utils.ProbabilityUtils.generateBidirectionalFriendshipSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/18/17.
 */
public class SpJ2RepartitionerTest {

    @Test
    public void testGetLogicalReplicationCount() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        SpJ2Repartitioner.State state = new SpJ2Repartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        int expectedReplication = replicas.get(1).size() + replicas.get(2).size() + replicas.get(3).size();
        assertTrue(expectedReplication == SpJ2Repartitioner.getLogicalReplicationCount(state));
    }

    @Test
    public void testFindPartner() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpJ2Repartitioner.State state = new SpJ2Repartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        Integer uid1 = 4;
        Integer expectedPartner = 12;
        Set<Integer> candidates = initSet(6, 7, 8, 9, 11, expectedPartner, 13); //omit friends and things on same partition

        Integer partnerId = SpJ2Repartitioner.findPartner(uid1, candidates, 2f, state);
        assertEquals(partnerId, expectedPartner);


        //Tricky...
        Integer trickyId1 = 1;
        Integer trickyPid1 = 1;
        Integer trickyId2 = 9;
        Integer trickyPid2 = 2;

        double currentReplicas = replicas.get(trickyPid1).size() + replicas.get(trickyPid2).size();
        double expectedIncreaseInReplicas = 2;

        double pivot = (currentReplicas + expectedIncreaseInReplicas) / currentReplicas;
        double differential = 0.00001;

        partnerId = SpJ2Repartitioner.findPartner(trickyId1, initSet(trickyId2), (float)(pivot - differential), state);
        assertTrue(partnerId == null);

        partnerId = SpJ2Repartitioner.findPartner(trickyId1, initSet(trickyId2), (float)(pivot + differential), state);
        assertEquals(partnerId, trickyId2);
    }

    @Test
    public void testGetSwapChangesThenSwapThenPhysicallyMigrate() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        SpJ2Repartitioner.State state = new SpJ2Repartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);


        //Find swap changes
        Integer uid1 = 1;
        Integer pid1 = 1;
        Integer uid2 = 6;
        Integer pid2 = 2;
        SpJ2Repartitioner.SwapChanges swapChanges = SpJ2Repartitioner.getSwapChanges(uid1, uid2, state);

        Set<Integer> expectedAddToP1 = initSet(1); //1 has friends on P1, so it needs a replica there
        Set<Integer> expectedAddToP2 = initSet(4, 6, 12); //6 has friends on P1, and 1 is friends with 4 and 12 (which aren't there yet)
        Set<Integer> expectedRemoveFromP1 = initSet(6); //we're moving 6 there, so no need for a replica
        Set<Integer> expectedRemoveFromP2 = initSet(1, 5); //1 will be a master, so no replica, and 5 was only there for 6

        assertEquals(swapChanges.getPid1(), pid1);
        assertEquals(swapChanges.getPid2(), pid2);
        assertEquals(swapChanges.getAddToP1(), expectedAddToP1);
        assertEquals(swapChanges.getAddToP2(), expectedAddToP2);
        assertEquals(swapChanges.getRemoveFromP1(), expectedRemoveFromP1);
        assertEquals(swapChanges.getRemoveFromP2(), expectedRemoveFromP2);


        //Swap
        SpJ2Repartitioner.swap(uid1, uid2, state);

        assertEquals(state.getLogicalPids().get(uid1), pid2);
        assertEquals(state.getLogicalPids().get(uid2), pid1);

        for(Integer addedToPid1 : swapChanges.getAddToP1()) {
            assertTrue(state.getLogicalReplicaPids().get(addedToPid1).contains(pid1));
        }
        for(Integer addedToPid2 : swapChanges.getAddToP2()) {
            assertTrue(state.getLogicalReplicaPids().get(addedToPid2).contains(pid2));
        }

        for(Integer removedFromPid1 : swapChanges.getRemoveFromP1()) {
            assertFalse(state.getLogicalReplicaPids().get(removedFromPid1).contains(pid1));
        }
        for(Integer removedFromPid2 : swapChanges.getRemoveFromP2()) {
            assertFalse(state.getLogicalReplicaPids().get(removedFromPid2).contains(pid2));
        }

        assertTrue(state.getLogicalReplicaPartitions().get(pid1).containsAll(swapChanges.getAddToP1()));
        assertTrue(state.getLogicalReplicaPartitions().get(pid2).containsAll(swapChanges.getAddToP2()));

        for(Integer removedFromPid1 : swapChanges.getRemoveFromP1()) {
            assertFalse(state.getLogicalReplicaPartitions().get(pid1).contains(removedFromPid1));
        }
        for(Integer removedFromPid2 : swapChanges.getRemoveFromP2()) {
            assertFalse(state.getLogicalReplicaPartitions().get(pid2).contains(removedFromPid2));
        }


        //Physically migrate
        SpJ2Repartitioner repartitioner = new SpJ2Repartitioner(manager, minNumReplicas, alpha, initialT, deltaT, k);
        repartitioner.physicallyMigrate(state.getLogicalPids(), state.getLogicalReplicaPids());

        RepUser user1Master = manager.getUserMasterById(uid1);
        RepUser user2Master = manager.getUserMasterById(uid2);
        SpJ2Partition partition1 = manager.getPartitionById(pid1);
        SpJ2Partition partition2 = manager.getPartitionById(pid2);

        //Check master partitions
        assertTrue  (partition1.getIdsOfMasters().contains(uid2));
        assertTrue  (partition2.getIdsOfMasters().contains(uid1));

        assertFalse (partition1.getIdsOfMasters().contains(uid1));
        assertFalse (partition2.getIdsOfMasters().contains(uid2));

        assertEquals(user1Master.getBasePid(), pid2);
        assertEquals(user2Master.getBasePid(), pid1);

        for(Integer replicaPid : user1Master.getReplicaPids()) {
            assertEquals(manager.getPartitionById(replicaPid).getReplicaById(uid1).getBasePid(), pid2);
        }

        for(Integer replicaPid : user2Master.getReplicaPids()) {
            assertEquals(manager.getPartitionById(replicaPid).getReplicaById(uid2).getBasePid(), pid1);
        }

        //Check replica partitions
        assertTrue(partition1.getIdsOfReplicas().containsAll(swapChanges.getAddToP1()));
        assertTrue(partition2.getIdsOfReplicas().containsAll(swapChanges.getAddToP2()));

        for(Integer shouldNotBeIn1 : swapChanges.getRemoveFromP1()) {
            assertFalse(partition1.getIdsOfReplicas().contains(shouldNotBeIn1));
        }
        for(Integer shouldNotBeIn2 : swapChanges.getRemoveFromP2()) {
            assertFalse(partition2.getIdsOfReplicas().contains(shouldNotBeIn2));
        }


        for(Integer shouldBeIn1 : swapChanges.getAddToP1()) {
            RepUser user = manager.getUserMasterById(shouldBeIn1);
            assertTrue(user.getReplicaPids().contains(pid1));
            for(Integer replicaPid : user.getReplicaPids()) {
                RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(shouldBeIn1);
                assertTrue(replica.getReplicaPids().contains(pid1));
            }
        }

        for(Integer shouldBeIn2 : swapChanges.getAddToP2()) {
            RepUser user = manager.getUserMasterById(shouldBeIn2);
            assertTrue(user.getReplicaPids().contains(pid2));
            for(Integer replicaPid : user.getReplicaPids()) {
                RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(shouldBeIn2);
                assertTrue(replica.getReplicaPids().contains(pid2));
            }
        }

        for(Integer shouldNotBeIn1 : swapChanges.getRemoveFromP1()) {
            RepUser user = manager.getUserMasterById(shouldNotBeIn1);
            assertFalse(user.getReplicaPids().contains(pid1));
            for(Integer replicaPid : user.getReplicaPids()) {
                RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(shouldNotBeIn1);
                assertFalse(replica.getReplicaPids().contains(pid1));
            }
        }

        for(Integer shouldNotBeIn2 : swapChanges.getRemoveFromP2()) {
            RepUser user = manager.getUserMasterById(shouldNotBeIn2);
            assertFalse(user.getReplicaPids().contains(pid2));
            for(Integer replicaPid : user.getReplicaPids()) {
                RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(shouldNotBeIn2);
                assertFalse(replica.getReplicaPids().contains(pid2));
            }
        }
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpJ2Repartitioner.State state = new SpJ2Repartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //For every friend of the moving user:
        //    We should add a replica in the target partition if there isn't already a replica or master in that partition
        assertEquals(findReplicasToAddToTargetPartition(1, 2, state), initSet(4, 12));
        assertEquals(findReplicasToAddToTargetPartition(1, 3, state), initSet(6, 8));
        assertEquals(findReplicasToAddToTargetPartition(13, 1, state), Collections.<Integer>emptySet());
    }

    @Test
    public void testFindReplicasInMovingPartitionToDelete() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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
        friendships.put(11, initSet(9, 12));
        friendships.put(12, initSet(13));
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpJ2Repartitioner.State state = new SpJ2Repartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //For every friend of the moving user, if:
        //    (1) friend has a replica on movingPartition
        //    (2) friend doesn't have any other friends on movingPartition
        //    (3) friend has more than minNumReplicas replicas
        //then delete it

        //All of its friends either have masters in P1, or have replicas in P1
        Set<Integer> toDeleteIn1For1 = findReplicasInMovingPartitionToDelete(1, 1, findReplicasToAddToTargetPartition(1, 2, state), state);
        assertEquals(toDeleteIn1For1, Collections.<Integer>emptySet());

        //10 was only in P2 for 9's sake
        Set<Integer> toDeleteIn2For9 = findReplicasInMovingPartitionToDelete(9, 2, findReplicasToAddToTargetPartition(9, 3, state), state);
        assertEquals(toDeleteIn2For9, initSet(10));

        //10's friends are 1, 4, 9, and 11.  1 and 9 have other friends on P3.  11 is on P3.  4's only there for 10, but would violate minNumReplicas
        Set<Integer> toDeleteIn3For10 = findReplicasInMovingPartitionToDelete(10, 3, findReplicasToAddToTargetPartition(10, 1, state), state);
        assertEquals(toDeleteIn3For10, Collections.<Integer>emptySet());
    }

    @Test
    public void testShouldDeleteReplicaInTargetPartitionAndShouldWeAddAReplicaOfMovingUserInMovingPartition() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpJ2Repartitioner.State state = new SpJ2Repartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //Test shouldDeleteReplicaInTargetPartition
        //If there's a replica of moving user in targetPartition, then delete it
        for(Integer pid : replicas.keySet()) {
            for(Integer uid : friendships.keySet()) {
                if(partitions.get(pid).contains(uid)) {
                    continue;
                }
                if(replicas.get(pid).contains(uid)) {
                    assertTrue(shouldDeleteReplicaInTargetPartition(uid, pid, state));
                }
                else {
                    assertFalse(shouldDeleteReplicaInTargetPartition(uid, pid, state));
                }
            }
        }

        //Test shouldWeAddAReplicaOfMovingUserInMovingPartition
        //We should add a replica if moving user has friends in moving partition
        //Additionally, we should add one if moving user has minNumReplica replicas, including one in targetPartition
        shouldWeAddAReplicaOfMovingUserInMovingPartition(1, 1, state);
        for(Integer pid : replicas.keySet()) {
uidLoop:    for (Integer uid : friendships.keySet()) {
                Integer logicalPid = state.getLogicalPids().get(uid);
                if(partitions.get(pid).contains(uid)) {
                    continue;
                }
                if(state.getLogicalReplicaPids().get(uid).contains(uid) && state.getLogicalReplicaPids().get(uid).size() <= state.getMinNumReplicas()) {
                    assertTrue(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
                    continue;
                }
                for(Integer friendId : state.getFriendships().get(uid)) {
                    if(state.getLogicalPids().get(friendId).equals(logicalPid)) {
                        assertTrue(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
                        continue uidLoop;
                    }
                }
                assertFalse(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
            }
        }
    }


    private static void fillState(SpJ2Repartitioner.State state, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        Map<Integer, Integer> logicalPids = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            for(Integer uid : partitions.get(pid)) {
                logicalPids.put(uid, pid);
            }
        }

        Map<Integer, Set<Integer>> logicalReplicaPids = new HashMap<>();
        for(Integer pid : replicas.keySet()) {
            for(Integer uid : replicas.get(pid)) {
                if(!logicalReplicaPids.containsKey(uid)) {
                    logicalReplicaPids.put(uid, new HashSet<Integer>());
                }
                logicalReplicaPids.get(uid).add(pid);
            }
        }

        state.setLogicalPids(logicalPids);
        state.setLogicalReplicaPids(logicalReplicaPids);
        state.setLogicalReplicaPartitions(replicas);
    }
}
