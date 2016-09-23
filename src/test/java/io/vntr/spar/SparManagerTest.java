package io.vntr.spar;

import io.vntr.User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        assertTrue(manager.getNumUsers() == 1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        assertTrue(manager.getNumUsers() == 2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        assertTrue(manager.getNumUsers() == 3);
    }

    @Test
    public void testGetAllPartitionIds() {
        SparManager manager = new SparManager(2);
        Long firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Long secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Long thirdId = manager.addPartition();
        SparPartition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Long fourthId = manager.addPartition();
        SparPartition fourthPartition = manager.getPartitionById(fourthId);
        assertTrue(fourthPartition.getId().equals(fourthId));

        assertNotEquals(firstId, secondId);
        assertNotEquals(firstId, thirdId);
        assertNotEquals(firstId, fourthId);
        assertNotEquals(secondId, thirdId);
        assertNotEquals(secondId, fourthId);
        assertNotEquals(thirdId, fourthId);

        Set<Long> partitionIds = manager.getAllPartitionIds();

        assertTrue(partitionIds.size() == 4);
        assertTrue(partitionIds.contains(firstId));
        assertTrue(partitionIds.contains(secondId));
        assertTrue(partitionIds.contains(thirdId));
        assertTrue(partitionIds.contains(fourthId));
    }

    @Test
    public void testAddUser() {
        SparManager manager = new SparManager(2);
        Long firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Long secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Long thirdId = manager.addPartition();
        SparPartition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Long fourthId = manager.addPartition();
        SparPartition fourthPartition = manager.getPartitionById(fourthId);
        assertTrue(fourthPartition.getId().equals(fourthId));

        assertNotEquals(firstId, secondId);
        assertNotEquals(firstId, thirdId);
        assertNotEquals(firstId, fourthId);
        assertNotEquals(secondId, thirdId);
        assertNotEquals(secondId, fourthId);
        assertNotEquals(thirdId, fourthId);

        Long userId = 23L;
        User user = new User("Anita", userId);
        manager.addUser(user);

        SparUser sparUser = manager.getUserMasterById(userId);
        assertEquals(sparUser.getName(), user.getName());
        assertEquals(sparUser.getId(), user.getId());

        assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
        for (Long partitionId : manager.getAllPartitionIds()) {
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        for (Long partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            assertTrue(partition.getNumMasters() == 0 || partition.getNumMasters() == 1);
        }

        for (Long userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getName(), user.getName());
            assertEquals(sparUser.getId(), user.getId());
            assertEquals(sparUser.getPartitionId(), sparUser.getMasterPartitionId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Long partitionId : manager.getAllPartitionIds()) {
                SparPartition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
                if (shouldContainReplica) {
                    System.out.println("Boom");
                    SparUser replica = partition.getReplicaById(userId);
                    assertEquals(replica.getId(), sparUser.getId());
                    assertEquals(replica.getName(), sparUser.getName());
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

        for (Long partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            assertFalse(partition.getIdsOfMasters().contains(userId1));
            assertFalse(partition.getIdsOfReplicas().contains(userId1));
        }

        userIdToUserMap.remove(userId1);
        for (Long userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getName(), user.getName());
            assertEquals(sparUser.getId(), user.getId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Long partitionId : manager.getAllPartitionIds()) {
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
        Long firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Long secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        assertNotEquals(firstId, secondId);
    }

    @Test
    public void testRemovePartition() {
        SparManager manager = new SparManager(2);
        Long firstId = manager.addPartition();
        SparPartition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Long secondId = manager.addPartition();
        SparPartition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Long thirdId = manager.addPartition();
        SparPartition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        manager.removePartition(secondId);

        Set<Long> partitionIds = manager.getAllPartitionIds();
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);


        for (Long userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getName(), user.getName());
            assertEquals(sparUser.getId(), user.getId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Long partitionId : manager.getAllPartitionIds()) {
                SparPartition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }

        assertTrue(manager.getNumUsers() == 3);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Long newPartitionId = manager.getRandomPartitionIdWhereThisUserIsNotPresent(sparUser1);
        assertFalse(sparUser1.getReplicaPartitionIds().contains(newPartitionId));
        manager.addReplica(manager.getUserMasterById(userId1), newPartitionId);

        assertTrue(sparUser1.getReplicaPartitionIds().size() == manager.getMinNumReplicas() + 1);
        for (Long partitionId : manager.getAllPartitionIds()) {
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);


        for (Long userId : userIdToUserMap.keySet()) {
            User user = userIdToUserMap.get(userId);
            SparUser sparUser = manager.getUserMasterById(userId);
            assertEquals(sparUser.getName(), user.getName());
            assertEquals(sparUser.getId(), user.getId());

            assertTrue(sparUser.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
            for (Long partitionId : manager.getAllPartitionIds()) {
                SparPartition partition = manager.getPartitionById(partitionId);
                boolean shouldContainReplica = sparUser.getReplicaPartitionIds().contains(partitionId);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }

        assertTrue(manager.getNumUsers() == 3);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Long newPartitionId = manager.getRandomPartitionIdWhereThisUserIsNotPresent(sparUser1);
        assertFalse(sparUser1.getReplicaPartitionIds().contains(newPartitionId));
        manager.addReplica(manager.getUserMasterById(userId1), newPartitionId);

        assertTrue(sparUser1.getReplicaPartitionIds().size() == manager.getMinNumReplicas() + 1);
        for (Long partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            boolean shouldContainReplica = sparUser1.getReplicaPartitionIds().contains(partitionId);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }

        Set<Long> replicaSetCopy = new HashSet<Long>(sparUser1.getReplicaPartitionIds());
        replicaSetCopy.remove(newPartitionId);
        Long replicaPartitionToRemove = replicaSetCopy.iterator().next();

        manager.removeReplica(sparUser1, replicaPartitionToRemove);

        assertFalse(sparUser1.getReplicaPartitionIds().contains(replicaPartitionToRemove));
        assertTrue(sparUser1.getReplicaPartitionIds().size() == manager.getMinNumReplicas());
        for (Long partitionId : manager.getAllPartitionIds()) {
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        SparUser sparUser3 = manager.getUserMasterById(userId3);

        manager.befriend(sparUser3, sparUser1);

        Long oldPartitionId = sparUser1.getMasterPartitionId();
        Set<Long> oldReplicaIds = sparUser1.getReplicaPartitionIds();

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(oldPartitionId).getMasterById(userId1));
        for (Long partitionId : manager.getAllPartitionIds()) {
            if (!partitionId.equals(oldPartitionId)) {
                assertNull(manager.getPartitionById(partitionId).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for (Long partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getMasterPartitionId(), oldPartitionId);
            assertEquals(replica.getReplicaPartitionIds(), oldReplicaIds);
        }

        Long newPartitionId = manager.getPartitionIdWithFewestMasters();
        Set<Long> replicasToAddInDestinationPartition = new HashSet<Long>();
        for (Long friendId : sparUser1.getFriendIDs()) {
            SparUser friend = manager.getUserMasterById(friendId);
            if (!friend.getMasterPartitionId().equals(newPartitionId) && !friend.getReplicaPartitionIds().contains(newPartitionId)) {
                replicasToAddInDestinationPartition.add(friendId);
            }
        }

        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);
        Set<Long> replicasToDeleteInSourcePartition = new HashSet<Long>(strategy.findReplicasInMovingPartitionToDelete(sparUser1, replicasToAddInDestinationPartition));
        manager.moveUser(sparUser1, newPartitionId, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);

        SparUser sparUser1Again = manager.getUserMasterById(userId1);
        Set<Long> expectedReplicaIds = new HashSet<Long>(oldReplicaIds);
        expectedReplicaIds.remove(newPartitionId);
        assertEquals(expectedReplicaIds, sparUser1Again.getReplicaPartitionIds());
        assertEquals(newPartitionId, sparUser1Again.getMasterPartitionId());
        assertEquals(sparUser1Again.getPartitionId(), sparUser1Again.getMasterPartitionId());

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(newPartitionId).getMasterById(userId1));
        for (Long partitionId : manager.getAllPartitionIds()) {
            if (!partitionId.equals(newPartitionId)) {
                assertNull(manager.getPartitionById(partitionId).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for (Long partitionId : sparUser1Again.getReplicaPartitionIds()) {
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        SparUser sparUser3 = manager.getUserMasterById(userId3);

        //This should not be true, as it would violate our balance constraint
        assertNotEquals(sparUser1.getMasterPartitionId(), sparUser3.getMasterPartitionId());

        manager.befriend(sparUser3, sparUser1);

        assertTrue(sparUser1.getFriendIDs().contains(userId3));
        assertTrue(sparUser3.getFriendIDs().contains(userId1));

        for (Long partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
        }

        for (Long partitionId : sparUser3.getReplicaPartitionIds()) {
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
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

        for (Long partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertTrue(replica.getFriendIDs().contains(userId2));
        }

        for (Long partitionId : sparUser2.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId2);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        for (Long partitionId : sparUser3.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        manager.unfriend(sparUser2, sparUser1);

        assertTrue(sparUser1.getFriendIDs().contains(userId3));
        assertFalse(sparUser1.getFriendIDs().contains(userId2));
        assertFalse(sparUser2.getFriendIDs().contains(userId1));
        assertTrue(sparUser3.getFriendIDs().contains(userId1));

        for (Long partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertFalse(replica.getFriendIDs().contains(userId2));
        }

        for (Long partitionId : sparUser2.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId2);
            assertFalse(replica.getFriendIDs().contains(userId1));
        }

        for (Long partitionId : sparUser3.getReplicaPartitionIds()) {
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Long user1OriginalMasterPartitionId = sparUser1.getMasterPartitionId();
        Set<Long> user1OriginalReplicaIds = new HashSet<Long>(sparUser1.getReplicaPartitionIds());

        for (Long partitionId : sparUser1.getReplicaPartitionIds()) {
            SparUser replica = manager.getPartitionById(partitionId).getReplicaById(userId1);
            assertEquals(replica.getMasterPartitionId(), user1OriginalMasterPartitionId);
            assertEquals(replica.getReplicaPartitionIds(), user1OriginalReplicaIds);
        }

        Long user1NewMasterPartitionId = sparUser1.getReplicaPartitionIds().iterator().next();
        assertNotEquals(user1NewMasterPartitionId, user1OriginalMasterPartitionId);
        manager.promoteReplicaToMaster(userId1, user1NewMasterPartitionId);

        Set<Long> user1NewReplicaIds = new HashSet<Long>(user1OriginalReplicaIds);
        user1NewReplicaIds.remove(user1NewMasterPartitionId);
        SparUser sparUser1Again = manager.getUserMasterById(userId1);
        assertEquals(sparUser1Again.getMasterPartitionId(), user1NewMasterPartitionId);
        assertEquals(sparUser1Again.getPartitionId(), user1NewMasterPartitionId);
        assertEquals(sparUser1Again.getReplicaPartitionIds(), user1NewReplicaIds);

        for (Long partitionId : sparUser1Again.getReplicaPartitionIds()) {
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

        Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
        Long userId1 = 23L;
        User user1 = new User("Anita", userId1);
        manager.addUser(user1);
        userIdToUserMap.put(userId1, user1);

        Long userId2 = 15L;
        User user2 = new User("Bob", userId2);
        manager.addUser(user2);
        userIdToUserMap.put(userId2, user2);

        Long userId3 = 2L;
        User user3 = new User("Carol", userId3);
        manager.addUser(user3);
        userIdToUserMap.put(userId3, user3);

        Set<Long> partitionsWithoutThisUser = new HashSet<Long>();
        for (Long partitionId : manager.getAllPartitionIds()) {
            SparPartition partition = manager.getPartitionById(partitionId);
            if (!partition.getIdsOfMasters().contains(userId1) && !partition.getIdsOfReplicas().contains(userId1)) {
                partitionsWithoutThisUser.add(partitionId);
            }
        }

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        Long partitionIdWhereThisUserIsNotPresent = manager.getRandomPartitionIdWhereThisUserIsNotPresent(sparUser1);
        assertTrue(partitionsWithoutThisUser.contains(partitionIdWhereThisUserIsNotPresent));
    }

    @Test
    public void testGetPartitionsToAddInitialReplicas() {
        SparManager manager = new SparManager(2);
        manager.addPartition();
        manager.addPartition();
        Long thirdPartitionId = manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        Set<Long> initialReplicaLocations = manager.getPartitionsToAddInitialReplicas(thirdPartitionId);

        assertFalse(initialReplicaLocations.contains(thirdPartitionId));
    }

    @Test
    public void testExport() {
        //TODO: do this
    }

    @Test
    public void testGetEdgeCut() {
        //TODO: do this
    }
}