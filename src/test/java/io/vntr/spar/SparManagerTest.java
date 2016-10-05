package io.vntr.spar;

import io.vntr.User;

import java.util.*;

import org.junit.Test;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.*;

public class SparManagerTest {
    @Test
    public void testGetMinNumReplicas() {
        int expectedValue = 2;
        SparManager manager = new SparManager(expectedValue);
        assertTrue(manager.getMinNumReplicas() == expectedValue);
    }

    @Test
    public void testGetNumUsers() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        assertTrue(manager.getNumUsers() == 0);

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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
        SparManager manager = new SparManager(2);
        Integer firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        SparPartition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Integer fourthId = manager.addPartition();
        SparPartition fourthPartition = manager.getPartitionById(fourthId);
        assertTrue(fourthPartition.getId().equals(fourthId));

        assertNotEquals(firstId, secondId);
        assertNotEquals(firstId, thirdId);
        assertNotEquals(firstId, fourthId);
        assertNotEquals(secondId, thirdId);
        assertNotEquals(secondId, fourthId);
        assertNotEquals(thirdId, fourthId);

        Set<Integer> partitionIds = manager.getAllPartitionIds();

        assertTrue(partitionIds.size() == 4);
        assertTrue(partitionIds.contains(firstId));
        assertTrue(partitionIds.contains(secondId));
        assertTrue(partitionIds.contains(thirdId));
        assertTrue(partitionIds.contains(fourthId));
    }

    @Test
    public void testAddUser() {
        SparManager manager = new SparManager(2);
        Integer firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        SparPartition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Integer fourthId = manager.addPartition();
        SparPartition fourthPartition = manager.getPartitionById(fourthId);
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

        SparUser sparUser = manager.getUserMasterById(userId);
        assertEquals(sparUser.getId(), user.getId());

        assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
        for (Integer partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
            assertTrue(containsReplica == shouldContainReplica);
        }
    }

    @Test
    public void testRemoveUser() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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

        for (Integer partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            assertTrue(partition.getNumMasters() == 0 || partition.getNumMasters() == 1);
        }

        for (Integer userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getId(), user.getId());
            assertEquals(sparUser.getPartitionId(), sparUser.getMasterPartitionId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getAllPartitionIds()) {
                SparPartition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
                if (shouldContainReplica) {
                    System.out.println("Boom");
                    SparUser replica = partition.getReplicaById(userId);
                    assertEquals(replica.getId(), sparUser.getId());
                    assertEquals(replica.getMasterPartitionId(), sparUser.getMasterPartitionId());
                    assertEquals(replica.getReplicaPartitionIds(), sparUser.getReplicaPartitionIds());
                    assertEquals(replica.getPartitionId(), partitionId);
                }
            }
        }

        assertTrue(manager.getNumUsers() == 3);
        manager.removeUser(userId1);
        assertTrue(manager.getNumUsers() == 2);

        assertNull(manager.getUserMasterById(userId1));

        for (Integer partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            assertFalse(partition.getIdsOfMasters().contains(userId1));
            assertFalse(partition.getIdsOfReplicas().contains(userId1));
        }

        userIdToUserMap.remove(userId1);
        for (Integer userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getId(), user.getId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getAllPartitionIds()) {
                SparPartition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }
    }

    @Test
    public void testAddPartition() {
        SparManager manager = new SparManager(2);
        Integer firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        assertNotEquals(firstId, secondId);
    }

    @Test
    public void testRemovePartition() {
        SparManager manager = new SparManager(2);
        Integer firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        SparPartition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        manager.removePartition(secondId);

        Set<Integer> partitionIds = manager.getAllPartitionIds();
        assertTrue(partitionIds.size() == 2);
        assertTrue(partitionIds.contains(firstId));
        assertTrue(partitionIds.contains(thirdId));
    }

    @Test
    public void testAddReplica() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getId(), user.getId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getAllPartitionIds()) {
                SparPartition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }

        assertTrue(manager.getNumUsers() == 3);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Integer newPartitionId = manager.getRandomPartitionIdWhereThisUserIsNotPresent(sparUser1);
        assertFalse(sparUser1.getReplicaPartitionIds().contains(newPartitionId));
        manager.addReplica(manager.getUserMasterById(userId1), newPartitionId);

        assertTrue(sparUser1.getReplicaPartitionIds().size() == manager.getMinNumReplicas() + 1);
        for (Integer partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = sparUser1.getReplicaPartitionIds().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }
    }

    @Test
    public void testRemoveReplica() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getId(), user.getId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Integer partitionId : manager.getAllPartitionIds()) {
                SparPartition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }

        assertTrue(manager.getNumUsers() == 3);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Integer newPartitionId = manager.getRandomPartitionIdWhereThisUserIsNotPresent(sparUser1);
        assertFalse(sparUser1.getReplicaPartitionIds().contains(newPartitionId));
        manager.addReplica(manager.getUserMasterById(userId1), newPartitionId);

        assertTrue(sparUser1.getReplicaPartitionIds().size() == manager.getMinNumReplicas() + 1);
        for (Integer partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = sparUser1.getReplicaPartitionIds().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }

        Set<Integer> replicaSetCopy = new HashSet<Integer>(sparUser1.getReplicaPartitionIds());
        replicaSetCopy.remove(newPartitionId);
        Integer replicaPartitionToRemove = replicaSetCopy.iterator().next();

        manager.removeReplica(sparUser1, replicaPartitionToRemove);

        assertFalse(sparUser1.getReplicaPartitionIds().contains(replicaPartitionToRemove));
        assertTrue(sparUser1.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
        for (Integer partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = sparUser1.getReplicaPartitionIds().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }
    }

    @Test
    public void testMoveUser() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        SparUser sparUser3 = manager.getUserMasterById(userId3);

        manager.befriend(sparUser3, sparUser1);

        Integer oldPartitionId = sparUser1.getMasterPartitionId();
        Set<Integer> oldReplicaIds = sparUser1.getReplicaPartitionIds();

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(oldPartitionId).getMasterById(userId1));
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (!partitionId.equals(oldPartitionId)) {
                assertNull(manager.getPartitionById(partitionId).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for (Integer partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getMasterPartitionId(), oldPartitionId);
            assertEquals(replica.getReplicaPartitionIds(), oldReplicaIds);
        }

        Integer newPartitionId = manager.getPartitionIdWithFewestMasters();
        Set<Integer> replicasToAddInDestinationPartition = new HashSet<Integer>();
        for (Integer friendId : sparUser1.getFriendIDs()) {
            SparUser friend = manager.getUserMasterById(friendId);
            if (!friend.getMasterPartitionId().equals(newPartitionId) && !friend.getReplicaPartitionIds().contains(newPartitionId)) {
                replicasToAddInDestinationPartition.add(friendId);
            }
        }

        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);
        Set<Integer> replicasToDeleteInSourcePartition = new HashSet<Integer>(strategy.findReplicasInMovingPartitionToDelete(sparUser1, replicasToAddInDestinationPartition));
        manager.moveUser(sparUser1, newPartitionId, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);

        SparUser sparUser1Again = manager.getUserMasterById(userId1);
        Set<Integer> expectedReplicaIds = new HashSet<Integer>(oldReplicaIds);
        expectedReplicaIds.remove(newPartitionId);
        assertEquals(expectedReplicaIds, sparUser1Again.getReplicaPartitionIds());
        assertEquals(newPartitionId, sparUser1Again.getMasterPartitionId());
        assertEquals(sparUser1Again.getPartitionId(), sparUser1Again.getMasterPartitionId());

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(newPartitionId).getMasterById(userId1));
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (!partitionId.equals(newPartitionId)) {
                assertNull(manager.getPartitionById(partitionId).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for (Integer partitionId : sparUser1Again.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getMasterPartitionId(), newPartitionId);
            assertEquals(replica.getReplicaPartitionIds(), expectedReplicaIds);
        }
    }

    @Test
    public void testBefriend() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        SparUser sparUser3 = manager.getUserMasterById(userId3);

        //This should not be true, as it would violate our balance constraint
        assertNotEquals(sparUser1.getMasterPartitionId(), sparUser3.getMasterPartitionId());

        manager.befriend(sparUser3, sparUser1);

        assertTrue(sparUser1.getFriendIDs().contains(userId3));
        assertTrue(sparUser3.getFriendIDs().contains(userId1));

        for (Integer partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
        }

        for (Integer partitionId : sparUser3.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }
    }

    @Test
    public void testUnfriend() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        SparUser sparUser2 = manager.getUserMasterById(userId2);
        SparUser sparUser3 = manager.getUserMasterById(userId3);

        //This should not be true, as it would violate our balance constraint
        assertNotEquals(sparUser1.getMasterPartitionId(), sparUser3.getMasterPartitionId());

        manager.befriend(sparUser3, sparUser1);
        manager.befriend(sparUser2, sparUser1);

        assertTrue(sparUser1.getFriendIDs().contains(userId3));
        assertTrue(sparUser1.getFriendIDs().contains(userId2));
        assertTrue(sparUser2.getFriendIDs().contains(userId1));
        assertTrue(sparUser3.getFriendIDs().contains(userId1));

        for (Integer partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertTrue(replica.getFriendIDs().contains(userId2));
        }

        for (Integer partitionId : sparUser2.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId2);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        for (Integer partitionId : sparUser3.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        manager.unfriend(sparUser2, sparUser1);

        assertTrue(sparUser1.getFriendIDs().contains(userId3));
        assertFalse(sparUser1.getFriendIDs().contains(userId2));
        assertFalse(sparUser2.getFriendIDs().contains(userId1));
        assertTrue(sparUser3.getFriendIDs().contains(userId1));

        for (Integer partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertFalse(replica.getFriendIDs().contains(userId2));
        }

        for (Integer partitionId : sparUser2.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId2);
            assertFalse(replica.getFriendIDs().contains(userId1));
        }

        for (Integer partitionId : sparUser3.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }
    }

    @Test
    public void testPromoteReplicaToMaster() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Integer user1OriginalMasterPartitionId = sparUser1.getMasterPartitionId();
        Set<Integer> user1OriginalReplicaIds = new HashSet<Integer>(sparUser1.getReplicaPartitionIds());

        for (Integer partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getMasterPartitionId(), user1OriginalMasterPartitionId);
            assertEquals(replica.getReplicaPartitionIds(), user1OriginalReplicaIds);
        }

        Integer user1NewMasterPartitionId = sparUser1.getReplicaPartitionIds().iterator().next();
        assertNotEquals(user1NewMasterPartitionId, user1OriginalMasterPartitionId);
        manager.promoteReplicaToMaster(userId1, user1NewMasterPartitionId);

        Set<Integer> user1NewReplicaIds = new HashSet<Integer>(user1OriginalReplicaIds);
        user1NewReplicaIds.remove(user1NewMasterPartitionId);
        SparUser sparUser1Again = manager.getUserMasterById(userId1);
        assertEquals(sparUser1Again.getMasterPartitionId(), user1NewMasterPartitionId);
        assertEquals(sparUser1Again.getPartitionId(), user1NewMasterPartitionId);
        assertEquals(sparUser1Again.getReplicaPartitionIds(), user1NewReplicaIds);

        for (Integer partitionId : sparUser1Again.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getMasterPartitionId(), user1NewMasterPartitionId);
            assertEquals(replica.getReplicaPartitionIds(), user1NewReplicaIds);
        }
    }

    public void testGetPartitionIdWithFewestMasters() {
        //don't really care, actually
    }

    @Test
    public void testGetRandomPartitionIdWhereThisUserIsNotPresent() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
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

        Set<Integer> partitionsWithoutThisUser = new HashSet<Integer>();
        for (Integer partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            if (!partition.getIdsOfMasters().contains(userId1) && !partition.getIdsOfReplicas().contains(userId1)) {
                partitionsWithoutThisUser.add(partitionId);
            }
        }

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Integer partitionIdWhereThisUserIsNotPresent = manager.getRandomPartitionIdWhereThisUserIsNotPresent(sparUser1);
        assertTrue(partitionsWithoutThisUser.contains(partitionIdWhereThisUserIsNotPresent));
    }

    @Test
    public void testGetPartitionsToAddInitialReplicas() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        Integer thirdPartitionId = manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        Set<Integer> initialReplicaLocations = manager.getPartitionsToAddInitialReplicas(thirdPartitionId);

        assertFalse(initialReplicaLocations.contains(thirdPartitionId));
    }

    @Test
    public void testExport() {
        //TODO: do this
    }

    @Test
    public void testGetEdgeCut() {
        int minNumReplicas = 0;
        Map<Integer, Set<Integer>> partitions = new HashMap<Integer, Set<Integer>>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<Integer, Set<Integer>>();
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

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<Integer, Set<Integer>>();
        replicaPartitions.put(1, Collections.<Integer>emptySet());
        replicaPartitions.put(2, Collections.<Integer>emptySet());
        replicaPartitions.put(3, Collections.<Integer>emptySet());

        SparManager manager = SparTestUtils.initGraph(minNumReplicas+1, partitions, friendships, replicaPartitions);

        assertEquals(manager.getEdgeCut(), (Integer) 25);

        replicaPartitions.put(1, initSet(15, 16, 17, 18,  19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5,  6,  7));
        replicaPartitions.put(3, initSet( 8,  9, 10, 11, 12, 13, 14));

        manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);

        assertEquals(manager.getEdgeCut(), (Integer) 25);
    }


}