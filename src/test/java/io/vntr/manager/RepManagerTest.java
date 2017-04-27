package io.vntr.manager;

import io.vntr.RepUser;
import io.vntr.User;

import java.util.*;

import io.vntr.befriend.SBefriender;
import org.junit.Test;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.InitUtils.initRepManager;
import static io.vntr.utils.Utils.*;
import static org.junit.Assert.*;

public class RepManagerTest {
    @Test
    public void testGetMinNumReplicas() {
        int expectedValue = 2;
        RepManager manager = new RepManager(expectedValue, 0);
        assertTrue(manager.getMinNumReplicas() == expectedValue);
    }

    @Test
    public void testGetNumUsers() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        assertTrue(manager.getNumUsers() == 0);

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        assertTrue(manager.getNumUsers() == 1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        assertTrue(manager.getNumUsers() == 2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        assertTrue(manager.getNumUsers() == 3);
    }

    @Test
    public void testGetAllPartitionIds() {
        RepManager manager = new RepManager(2, 0);
        Integer firstId = manager.addPartition();
        Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        Partition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Integer fourthId = manager.addPartition();
        Partition fourthPartition = manager.getPartitionById(fourthId);
        assertTrue(fourthPartition.getId().equals(fourthId));

        assertNotEquals(firstId, secondId);
        assertNotEquals(firstId, thirdId);
        assertNotEquals(firstId, fourthId);
        assertNotEquals(secondId, thirdId);
        assertNotEquals(secondId, fourthId);
        assertNotEquals(thirdId, fourthId);

        Set<Integer> partitionIds = manager.getPids();

        assertTrue(partitionIds.size() == 4);
        assertTrue(partitionIds.contains(firstId));
        assertTrue(partitionIds.contains(secondId));
        assertTrue(partitionIds.contains(thirdId));
        assertTrue(partitionIds.contains(fourthId));
    }

    @Test
    public void testAddUser() {
        RepManager manager = new RepManager(2, 0);
        Integer firstId = manager.addPartition();
        Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        Partition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Integer fourthId = manager.addPartition();
        Partition fourthPartition = manager.getPartitionById(fourthId);
        assertTrue(fourthPartition.getId().equals(fourthId));

        assertNotEquals(firstId, secondId);
        assertNotEquals(firstId, thirdId);
        assertNotEquals(firstId, fourthId);
        assertNotEquals(secondId, thirdId);
        assertNotEquals(secondId, fourthId);
        assertNotEquals(thirdId, fourthId);

        Integer userId = 23;
        User user = new User(userId);
        manager.addUser(user);

        RepUser RepUser = manager.getUserMaster(userId);
        assertEquals(RepUser.getId(), user.getId());

        assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
        for (Integer partitionId : manager.getPids()) {
            Partition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = RepUser.getReplicaPids().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
            assertTrue(containsReplica == shouldContainReplica);
        }
    }

    @Test
    public void testRemoveUser() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        for (Integer partitionId : manager.getPids()) {
            Partition partition = manager.getPartitionById(partitionId);
            assertTrue(partition.getNumMasters() == 0 || partition.getNumMasters() == 1);
        }

        for (Integer userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getPids()) {
                Partition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
                if (shouldContainReplica) {
                    RepUser replica = partition.getReplicaById(userId);
                    assertEquals(replica.getId(), RepUser.getId());
                    assertEquals(replica.getBasePid(), RepUser.getBasePid());
                    assertEquals(replica.getReplicaPids(), RepUser.getReplicaPids());
                }
            }
        }

        assertTrue(manager.getNumUsers() == 3);
        manager.removeUser(userId1);
        assertTrue(manager.getNumUsers() == 2);

        assertNull(manager.getUserMaster(userId1));

        for (Integer partitionId : manager.getPids()) {
            Partition partition = manager.getPartitionById(partitionId);
            assertFalse(partition.getIdsOfMasters().contains(userId1));
            assertFalse(partition.getIdsOfReplicas().contains(userId1));
        }

        userIdToUserMap.remove(userId1);
        for (Integer userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getPids()) {
                Partition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }
    }

    @Test
    public void testAddPartition() {
        RepManager manager = new RepManager(2, 0);
        Integer firstId = manager.addPartition();
        Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        assertNotEquals(firstId, secondId);
    }

    @Test
    public void testRemovePartition() {
        RepManager manager = new RepManager(2, 0);
        Integer firstId = manager.addPartition();
        Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        Partition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        manager.removePartition(secondId);

        Set<Integer> partitionIds = manager.getPids();
        assertTrue(partitionIds.size() == 2);
        assertTrue(partitionIds.contains(firstId));
        assertTrue(partitionIds.contains(thirdId));
    }

    @Test
    public void testAddReplica() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);


        for (Integer userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getPids()) {
                Partition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }

        assertTrue(manager.getNumUsers() == 3);

        RepUser RepUser1 = manager.getUserMaster(userId1);
        Integer newPartitionId = manager.getRandomPidWhereThisUserIsNotPresent(RepUser1);
        assertFalse(RepUser1.getReplicaPids().contains(newPartitionId));
        manager.addReplica(manager.getUserMaster(userId1), newPartitionId);

        assertTrue(RepUser1.getReplicaPids().size() == manager.getMinNumReplicas() + 1);
        for (Integer partitionId : manager.getPids()) {
            Partition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = RepUser1.getReplicaPids().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }
    }

    @Test
    public void testRemoveReplica() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);


        for (Integer userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getPids()) {
                Partition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }

        assertTrue(manager.getNumUsers() == 3);

        RepUser RepUser1 = manager.getUserMaster(userId1);
        Integer newPartitionId = manager.getRandomPidWhereThisUserIsNotPresent(RepUser1);
        assertFalse(RepUser1.getReplicaPids().contains(newPartitionId));
        manager.addReplica(manager.getUserMaster(userId1), newPartitionId);

        assertTrue(RepUser1.getReplicaPids().size() == manager.getMinNumReplicas() + 1);
        for (Integer partitionId : manager.getPids()) {
            Partition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = RepUser1.getReplicaPids().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }

        Set<Integer> replicaSetCopy = new HashSet<>(RepUser1.getReplicaPids());
        replicaSetCopy.remove(newPartitionId);
        Integer replicaPartitionToRemove = replicaSetCopy.iterator().next();

        manager.removeReplica(RepUser1, replicaPartitionToRemove);

        assertFalse(RepUser1.getReplicaPids().contains(replicaPartitionToRemove));
        assertTrue(RepUser1.getReplicaPids().size() == manager.getMinNumReplicas());
        for (Integer partitionId : manager.getPids()) {
            Partition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = RepUser1.getReplicaPids().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }
    }

    @Test
    public void testMoveUser() {
        int minNumReplicas = 2;
        RepManager manager = new RepManager(minNumReplicas, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        RepUser repUser1 = manager.getUserMaster(userId1);
        RepUser RepUser3 = manager.getUserMaster(userId3);

        manager.befriend(RepUser3, repUser1);

        Integer oldPartitionId = repUser1.getBasePid();
        Set<Integer> oldReplicaIds = repUser1.getReplicaPids();

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(oldPartitionId).getMasterById(userId1));
        for (Integer partitionId : manager.getPids()) {
            if (!partitionId.equals(oldPartitionId)) {
                assertNull(manager.getPartitionById(partitionId).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for (Integer partitionId : repUser1.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getBasePid(), oldPartitionId);
            assertEquals(replica.getReplicaPids(), oldReplicaIds);
        }

        Integer newPartitionId = manager.getPidWithFewestMasters();
        Set<Integer> replicasToAddInDestinationPartition = new HashSet<>();
        for (Integer friendId : repUser1.getFriendIDs()) {
            RepUser friend = manager.getUserMaster(friendId);
            if (!friend.getBasePid().equals(newPartitionId) && !friend.getReplicaPids().contains(newPartitionId)) {
                replicasToAddInDestinationPartition.add(friendId);
            }
        }

//        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);
//        Set<Integer> replicasToDeleteInSourcePartition = new HashSet<>(strategy.findReplicasInMovingPartitionToDelete(repUser1, replicasToAddInDestinationPartition));
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(manager.getPartitionToUserMap());
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(manager.getPartitionToReplicasMap(), manager.getUids());
        Set<Integer> replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(repUser1, replicasToAddInDestinationPartition, minNumReplicas, uidToReplicasMap, uidToPidMap, manager.getFriendships());
        manager.moveUser(repUser1, newPartitionId, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);

        RepUser RepUser1Again = manager.getUserMaster(userId1);
        Set<Integer> expectedReplicaIds = new HashSet<>(oldReplicaIds);
        expectedReplicaIds.remove(newPartitionId);
        assertEquals(expectedReplicaIds, RepUser1Again.getReplicaPids());
        assertEquals(newPartitionId, RepUser1Again.getBasePid());

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(newPartitionId).getMasterById(userId1));
        for (Integer partitionId : manager.getPids()) {
            if (!partitionId.equals(newPartitionId)) {
                assertNull(manager.getPartitionById(partitionId).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for (Integer partitionId : RepUser1Again.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getBasePid(), newPartitionId);
            assertEquals(replica.getReplicaPids(), expectedReplicaIds);
        }
    }

    @Test
    public void testBefriend() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        RepUser RepUser1 = manager.getUserMaster(userId1);
        RepUser RepUser3 = manager.getUserMaster(userId3);

        //This should not be true, as it would violate our balance constraint
        assertNotEquals(RepUser1.getBasePid(), RepUser3.getBasePid());

        manager.befriend(RepUser3, RepUser1);

        assertTrue(RepUser1.getFriendIDs().contains(userId3));
        assertTrue(RepUser3.getFriendIDs().contains(userId1));

        for (Integer partitionId : RepUser1.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
        }

        for (Integer partitionId : RepUser3.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }
    }

    @Test
    public void testUnfriend() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        RepUser RepUser1 = manager.getUserMaster(userId1);
        RepUser RepUser2 = manager.getUserMaster(userId2);
        RepUser RepUser3 = manager.getUserMaster(userId3);

        //This should not be true, as it would violate our balance constraint
        assertNotEquals(RepUser1.getBasePid(), RepUser3.getBasePid());

        manager.befriend(RepUser3, RepUser1);
        manager.befriend(RepUser2, RepUser1);

        assertTrue(RepUser1.getFriendIDs().contains(userId3));
        assertTrue(RepUser1.getFriendIDs().contains(userId2));
        assertTrue(RepUser2.getFriendIDs().contains(userId1));
        assertTrue(RepUser3.getFriendIDs().contains(userId1));

        for (Integer partitionId : RepUser1.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertTrue(replica.getFriendIDs().contains(userId2));
        }

        for (Integer partitionId : RepUser2.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId2);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        for (Integer partitionId : RepUser3.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        manager.unfriend(RepUser2, RepUser1);

        assertTrue(RepUser1.getFriendIDs().contains(userId3));
        assertFalse(RepUser1.getFriendIDs().contains(userId2));
        assertFalse(RepUser2.getFriendIDs().contains(userId1));
        assertTrue(RepUser3.getFriendIDs().contains(userId1));

        for (Integer partitionId : RepUser1.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertFalse(replica.getFriendIDs().contains(userId2));
        }

        for (Integer partitionId : RepUser2.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId2);
            assertFalse(replica.getFriendIDs().contains(userId1));
        }

        for (Integer partitionId : RepUser3.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }
    }

    @Test
    public void testPromoteReplicaToMaster() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        RepUser RepUser1 = manager.getUserMaster(userId1);
        Integer user1OriginalMasterPartitionId = RepUser1.getBasePid();
        Set<Integer> user1OriginalReplicaIds = new HashSet<>(RepUser1.getReplicaPids());

        for (Integer partitionId : RepUser1.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getBasePid(), user1OriginalMasterPartitionId);
            assertEquals(replica.getReplicaPids(), user1OriginalReplicaIds);
        }

        Integer user1NewMasterPartitionId = RepUser1.getReplicaPids().iterator().next();
        assertNotEquals(user1NewMasterPartitionId, user1OriginalMasterPartitionId);
        manager.promoteReplicaToMaster(userId1, user1NewMasterPartitionId);

        Set<Integer> user1NewReplicaIds = new HashSet<>(user1OriginalReplicaIds);
        user1NewReplicaIds.remove(user1NewMasterPartitionId);
        RepUser RepUser1Again = manager.getUserMaster(userId1);
        assertEquals(RepUser1Again.getBasePid(), user1NewMasterPartitionId);
        assertEquals(RepUser1Again.getReplicaPids(), user1NewReplicaIds);

        for (Integer partitionId : RepUser1Again.getReplicaPids()) {
            RepUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getBasePid(), user1NewMasterPartitionId);
            assertEquals(replica.getReplicaPids(), user1NewReplicaIds);
        }
    }

    public void testGetPartitionIdWithFewestMasters() {
        //don't really care, actually
    }

    @Test
    public void testGetRandomPartitionIdWhereThisUserIsNotPresent() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<>();
        Integer userId1 = 23;
        User user1 = new User(userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Integer userId2 = 15;
        User user2 = new User(userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Integer userId3 = 2;
        User user3 = new User(userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        Set<Integer> partitionsWithoutThisUser = new HashSet<>();
        for (Integer partitionId : manager.getPids()) {
            Partition partition = manager.getPartitionById(partitionId);
            if (!partition.getIdsOfMasters().contains(userId1) && !partition.getIdsOfReplicas().contains(userId1)) {
                partitionsWithoutThisUser.add(partitionId);
            }
        }

        RepUser RepUser1 = manager.getUserMaster(userId1);
        Integer partitionIdWhereThisUserIsNotPresent = manager.getRandomPidWhereThisUserIsNotPresent(RepUser1);
        assertTrue(partitionsWithoutThisUser.contains(partitionIdWhereThisUserIsNotPresent));
    }

    @Test
    public void testGetPartitionsToAddInitialReplicas() {
        RepManager manager = new RepManager(2, 0);
        manager.addPartition();
        manager.addPartition();
        Integer thirdPartitionId = manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        Set<Integer> initialReplicaLocations = manager.getPartitionsToAddInitialReplicas(thirdPartitionId);

        assertFalse(initialReplicaLocations.contains(thirdPartitionId));
    }

    @Test
    public void testGetEdgeCut() {
        int minNumReplicas = 0;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 20));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(14));
        friendships.put(14, initSet(15));
        friendships.put(15, initSet(16));
        friendships.put(16, initSet(17));
        friendships.put(17, initSet(18));
        friendships.put(18, initSet(19));
        friendships.put(19, initSet(20));
        friendships.put(20, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<>();
        replicaPartitions.put(1, Collections.<Integer>emptySet());
        replicaPartitions.put(2, Collections.<Integer>emptySet());
        replicaPartitions.put(3, Collections.<Integer>emptySet());

        RepManager manager = initRepManager(minNumReplicas+1, 0, partitions, friendships, replicaPartitions);

        assertEquals(manager.getEdgeCut(), (Integer) 25);

        replicaPartitions.put(1, initSet(15, 16, 17, 18,  19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5,  6,  7));
        replicaPartitions.put(3, initSet( 8,  9, 10, 11, 12, 13, 14));


        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);

        assertEquals(manager.getEdgeCut(), (Integer) 25);
    }


}