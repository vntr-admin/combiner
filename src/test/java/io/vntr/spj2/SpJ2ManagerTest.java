package io.vntr.spj2;

import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/18/17.
 */
public class SpJ2ManagerTest {
    @Test
    public void testAddUser() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        Integer newUid = manager.addUser();
        RepUser newUser = manager.getUserMasterById(newUid);
        Set<Integer> expectedUids = new HashSet<>(friendships.keySet());
        expectedUids.add(newUid);
        assertEquals(manager.getUids(), expectedUids);
        assertTrue(newUser.getFriendIDs().isEmpty());

        Integer newPid = newUser.getBasePid();
        assertTrue(manager.getPartitionById(newPid).getIdsOfMasters().contains(newUid));
        assertTrue(newUser.getReplicaPids().size() >= minNumReplicas);
        for(Integer replicaPid : newUser.getReplicaPids()) {
            assertTrue(manager.getPartitionById(replicaPid).getIdsOfReplicas().contains(newUid));
        }

        for(Integer originalUid : friendships.keySet()) {
            assertEquals(manager.getUserMasterById(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }


        Integer manualUid = new TreeSet<>(manager.getUids()).last() + 1;
        manager.addUser(new User(manualUid));
        RepUser manualUser = manager.getUserMasterById(manualUid);

        expectedUids.add(manualUid);
        assertEquals(manager.getUids(), expectedUids);
        assertTrue(manualUser.getFriendIDs().isEmpty());

        Integer manualPid = manualUser.getBasePid();
        assertTrue(manager.getPartitionById(manualPid).getIdsOfMasters().contains(manualUid));
        assertTrue(manualUser.getReplicaPids().size() >= minNumReplicas);
        for(Integer replicaPid : manualUser.getReplicaPids()) {
            assertTrue(manager.getPartitionById(replicaPid).getIdsOfReplicas().contains(manualUid));
        }

        for(Integer originalUid : friendships.keySet()) {
            assertEquals(manager.getUserMasterById(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }
    }

    @Test
    public void testRemoveUser() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        Integer uidToRemove = 6;

        manager.removeUser(uidToRemove);
        Set<Integer> expectedUids = new HashSet<>(friendships.keySet());
        expectedUids.remove(uidToRemove);
        assertEquals(manager.getUids(), expectedUids);

        for(Integer pid : manager.getPids()) {
            SpJ2Partition partition = manager.getPartitionById(pid);
            assertFalse(partition.getIdsOfMasters().contains(uidToRemove));
            assertFalse(partition.getIdsOfReplicas().contains(uidToRemove));

            for(Integer replicaId : partition.getIdsOfReplicas()) {
                RepUser replica = partition.getReplicaById(replicaId);
                assertFalse(replica.getFriendIDs().contains(uidToRemove));
            }
        }

        for(Integer uid : manager.getUids()) {
            assertFalse(manager.getUserMasterById(uid).getFriendIDs().contains(uidToRemove));
        }

        bidirectionalFriendships.remove(uidToRemove);
        for(Integer uid : bidirectionalFriendships.keySet()) {
            bidirectionalFriendships.get(uid).remove(uidToRemove);
        }

        assertEquals(bidirectionalFriendships, manager.getFriendships());
    }

    @Test
    public void testBefriend() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        Integer newFriendId1 = 6;
        Integer newFriendId2 = 13;
        manager.befriend(manager.getUserMasterById(newFriendId1), manager.getUserMasterById(newFriendId2));
        bidirectionalFriendships.get(newFriendId1).add(newFriendId2);
        bidirectionalFriendships.get(newFriendId2).add(newFriendId1);
        assertEquals(manager.getFriendships(), bidirectionalFriendships);

        RepUser newFriend1 = manager.getUserMasterById(newFriendId1);
        RepUser newFriend2 = manager.getUserMasterById(newFriendId2);

        assertTrue(newFriend1.getFriendIDs().contains(newFriendId2));
        assertTrue(newFriend2.getFriendIDs().contains(newFriendId1));

        for(Integer replicaPid : newFriend1.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(newFriendId1);
            assertTrue(replica.getFriendIDs().contains(newFriendId2));
        }

        for(Integer replicaPid : newFriend2.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(newFriendId2);
            assertTrue(replica.getFriendIDs().contains(newFriendId1));
        }
    }

    @Test
    public void testUnfriend() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        Integer unfriendId1 = 2;
        Integer unfriendId2 = 6;

        manager.unfriend(manager.getUserMasterById(unfriendId1), manager.getUserMasterById(unfriendId2));
        bidirectionalFriendships.get(unfriendId1).remove(unfriendId2);
        bidirectionalFriendships.get(unfriendId2).remove(unfriendId1);
        assertEquals(manager.getFriendships(), bidirectionalFriendships);

        RepUser formerFriend1 = manager.getUserMasterById(unfriendId1);
        RepUser formerFriend2 = manager.getUserMasterById(unfriendId2);

        assertFalse(formerFriend1.getFriendIDs().contains(unfriendId2));
        assertFalse(formerFriend2.getFriendIDs().contains(unfriendId1));

        for(Integer replicaPid : formerFriend1.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(unfriendId1);
            assertFalse(replica.getFriendIDs().contains(unfriendId2));
        }

        for(Integer replicaPid : formerFriend2.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(unfriendId2);
            assertFalse(replica.getFriendIDs().contains(unfriendId1));
        }
    }

    @Test
    public void testAddPartition() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        assertEquals(manager.getPids(), partitions.keySet());

        Integer newPid = manager.addPartition();

        Set<Integer> expectedPids = new HashSet<>(partitions.keySet());
        expectedPids.add(newPid);

        assertEquals(manager.getPids(), expectedPids);

        Integer pidToAdd = 1;
        for(; pidToAdd < 10; pidToAdd++) {
            if(!manager.getPids().contains(pidToAdd)) {
                break;
            }
        }

        expectedPids.add(pidToAdd);
        manager.addPartition(pidToAdd);
        assertEquals(manager.getPids(), expectedPids);
        assertEquals(manager.getFriendships(), bidirectionalFriendships);
    }

    @Test
    public void testRemovePartition() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);

        assertEquals(manager.getPids(), partitions.keySet());

        Integer pidToRemove = 2;

        manager.removePartition(pidToRemove);

        assertEquals(initSet(1, 3), manager.getPids());
        assertEquals(manager.getUids(), friendships.keySet());
        assertEquals(manager.getPartitionById(1).getIdsOfMasters(),  partitions.get(1));
        assertEquals(manager.getPartitionById(1).getIdsOfReplicas(), replicas.get(1));
        assertEquals(manager.getPartitionById(3).getIdsOfMasters(),  partitions.get(3));
        assertEquals(manager.getPartitionById(3).getIdsOfReplicas(), replicas.get(3));
    }

    @Test
    public void testAddReplica() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);

        Integer uidToReplicate = 4;
        Integer pidOnWhichToReplicate = 2;
        manager.addReplica(manager.getUserMasterById(uidToReplicate), pidOnWhichToReplicate);

        RepUser user = manager.getUserMasterById(uidToReplicate);
        assertEquals(user.getReplicaPids(), initSet(2, 3));
        for(Integer replicaPid : user.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(uidToReplicate);
            assertTrue(replica.getReplicaPids().contains(pidOnWhichToReplicate));
        }

        replicas.get(pidOnWhichToReplicate).add(uidToReplicate);
        assertEquals(manager.getPartitionToReplicasMap(), replicas);
    }

    @Test
    public void testRemoveReplica() {
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
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11,  4));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);

        Integer uidToDereplicate = 4;
        Integer pidOnWhichToDereplicate = 2;

        manager.removeReplica(manager.getUserMasterById(uidToDereplicate), pidOnWhichToDereplicate);


        RepUser user = manager.getUserMasterById(uidToDereplicate);
        assertEquals(user.getReplicaPids(), initSet( 3));
        for(Integer replicaPid : user.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(uidToDereplicate);
            assertTrue(replica.getReplicaPids().contains(replicaPid));
            assertFalse(replica.getReplicaPids().contains(pidOnWhichToDereplicate));
        }

        replicas.get(pidOnWhichToDereplicate).remove(uidToDereplicate);
        assertEquals(manager.getPartitionToReplicasMap(), replicas);
    }

    @Test
    public void testMoveMasterAndInformReplicas() {
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
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11,  4));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);

        Integer uidToMove = 1;
        Integer pidToMoveFrom = 1;
        Integer pidToMoveTo = 3;
        manager.moveMasterAndInformReplicas(uidToMove, pidToMoveFrom, pidToMoveTo);

        RepUser user = manager.getUserMasterById(uidToMove);
        assertEquals(user.getBasePid(), pidToMoveTo);
        Set<Integer> expectedReplicas = initSet(2, 3);
        assertEquals(user.getReplicaPids(), expectedReplicas);
        for(Integer replicaPid : user.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPid).getReplicaById(uidToMove);
            assertEquals(replica.getBasePid(), pidToMoveTo);
            assertEquals(replica.getReplicaPids(), expectedReplicas);
        }
    }

    @Test
    public void promoteReplicaToMaster() {
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

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, 0, partitions, friendships, replicas);
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        assertEquals(manager.getPartitionToReplicasMap(), replicas);

        Integer uidToPromote = 1;
        Integer pidInWhichToPromote = 3;
        manager.promoteReplicaToMaster(uidToPromote, pidInWhichToPromote);
        RepUser newMaster = manager.getUserMasterById(uidToPromote);
        assertEquals(newMaster.getBasePid(), pidInWhichToPromote);
        assertEquals(newMaster.getReplicaPids(), initSet(2));
        for(Integer replicaPids : newMaster.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(replicaPids).getReplicaById(uidToPromote);
            assertEquals(replica.getBasePid(), pidInWhichToPromote);
            assertEquals(replica.getReplicaPids(), initSet(2));
        }

        Set<Integer> friendsToReplicateInNewPartition = initSet(6, 8);

        replicas.get(3).remove(uidToPromote);
        replicas.get(3).addAll(friendsToReplicateInNewPartition);

        assertEquals(manager.getPartitionToReplicasMap(), replicas);

        for(Integer friendId : friendsToReplicateInNewPartition) {
            RepUser friend = manager.getUserMasterById(friendId);
            assertTrue(friend.getReplicaPids().contains(pidInWhichToPromote));
            for(Integer replicaPid : friend.getReplicaPids()) {
                RepUser friendReplica = manager.getPartitionById(replicaPid).getReplicaById(friendId);
                assertTrue(friendReplica.getReplicaPids().contains(pidInWhichToPromote));
            }
        }
    }


    @Test
    public void testPhysicallyMigrate() {
        //TODO: do this
    }
}
