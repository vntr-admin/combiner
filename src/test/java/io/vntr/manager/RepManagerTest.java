package io.vntr.manager;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.User;

import io.vntr.befriend.SBefriender;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static io.vntr.utils.InitUtils.initRepManager;
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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
        RepManager.Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        RepManager.Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        RepManager.Partition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Integer fourthId = manager.addPartition();
        RepManager.Partition fourthPartition = manager.getPartitionById(fourthId);
        assertTrue(fourthPartition.getId().equals(fourthId));

        assertNotEquals(firstId, secondId);
        assertNotEquals(firstId, thirdId);
        assertNotEquals(firstId, fourthId);
        assertNotEquals(secondId, thirdId);
        assertNotEquals(secondId, fourthId);
        assertNotEquals(thirdId, fourthId);

        TIntSet partitionIds = manager.getPids();

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
        RepManager.Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        RepManager.Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        RepManager.Partition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        Integer fourthId = manager.addPartition();
        RepManager.Partition fourthPartition = manager.getPartitionById(fourthId);
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
        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            RepManager.Partition partition = manager.getPartitionById(pid);
            boolean shouldContainReplica = RepUser.getReplicaPids().contains(pid);
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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

        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            RepManager.Partition partition = manager.getPartitionById(iter.next());
            assertTrue(partition.getNumMasters() == 0 || partition.getNumMasters() == 1);
        }

        for (Integer userId : userIdToUserMap.keys()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
                int pid = iter.next();
                RepManager.Partition partition = manager.getPartitionById(pid);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(pid);
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

        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            RepManager.Partition partition = manager.getPartitionById(iter.next());
            assertFalse(partition.getIdsOfMasters().contains(userId1));
            assertFalse(partition.getIdsOfReplicas().contains(userId1));
        }

        userIdToUserMap.remove(userId1);
        for (Integer userId : userIdToUserMap.keys()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
                int pid = iter.next();
                RepManager.Partition partition = manager.getPartitionById(pid);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(pid);
                boolean containsReplica = partition.getIdsOfReplicas().contains(userId);
                assertTrue(containsReplica == shouldContainReplica);
            }
        }
    }

    @Test
    public void testAddPartition() {
        RepManager manager = new RepManager(2, 0);
        Integer firstId = manager.addPartition();
        RepManager.Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        RepManager.Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        assertNotEquals(firstId, secondId);
    }

    @Test
    public void testRemovePartition() {
        RepManager manager = new RepManager(2, 0);
        Integer firstId = manager.addPartition();
        RepManager.Partition firstPartition = manager.getPartitionById(firstId);
        assertTrue(firstPartition.getId().equals(firstId));

        Integer secondId = manager.addPartition();
        RepManager.Partition secondPartition = manager.getPartitionById(secondId);
        assertTrue(secondPartition.getId().equals(secondId));

        Integer thirdId = manager.addPartition();
        RepManager.Partition thirdPartition = manager.getPartitionById(thirdId);
        assertTrue(thirdPartition.getId().equals(thirdId));

        manager.removePartition(secondId);

        TIntSet partitionIds = manager.getPids();
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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


        for (Integer userId : userIdToUserMap.keys()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
                int pid = iter.next();
                RepManager.Partition partition = manager.getPartitionById(pid);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(pid);
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
        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            RepManager.Partition partition = manager.getPartitionById(pid);
            boolean shouldContainReplica = RepUser1.getReplicaPids().contains(pid);
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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


        for (Integer userId : userIdToUserMap.keys()) {
            User user = userIdToUserMap.get(userId);
            RepUser RepUser = manager.getUserMaster(userId);
            assertEquals(RepUser.getId(), user.getId());

            assertTrue(RepUser.getReplicaPids().size() == manager.getMinNumReplicas());
            for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
                int pid = iter.next();
                RepManager.Partition partition = manager.getPartitionById(pid);
                boolean shouldContainReplica = RepUser.getReplicaPids().contains(pid);
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
        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            RepManager.Partition partition = manager.getPartitionById(pid);
            boolean shouldContainReplica = RepUser1.getReplicaPids().contains(pid);
            boolean containsReplica = partition.getIdsOfReplicas().contains(userId1);
            assertTrue(containsReplica == shouldContainReplica);
        }

        TIntSet replicaSetCopy = new TIntHashSet(RepUser1.getReplicaPids());
        replicaSetCopy.remove(newPartitionId);
        Integer replicaPartitionToRemove = replicaSetCopy.iterator().next();

        manager.removeReplica(RepUser1, replicaPartitionToRemove);

        assertFalse(RepUser1.getReplicaPids().contains(replicaPartitionToRemove));
        assertTrue(RepUser1.getReplicaPids().size() == manager.getMinNumReplicas());

        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            RepManager.Partition partition = manager.getPartitionById(pid);
            boolean shouldContainReplica = RepUser1.getReplicaPids().contains(pid);
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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
        TIntSet oldReplicaIds = repUser1.getReplicaPids();

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(oldPartitionId).getMasterById(userId1));
        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            if (pid != oldPartitionId) {
                assertNull(manager.getPartitionById(pid).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for(TIntIterator iter = repUser1.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId1);
            assertEquals(replica.getBasePid(), oldPartitionId);
            assertEquals(replica.getReplicaPids(), oldReplicaIds);
        }

        Integer newPartitionId = manager.getPidWithFewestMasters();
        TIntSet replicasToAddInDestinationPartition = new TIntHashSet();
        for(TIntIterator iter = repUser1.getFriendIDs().iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            RepUser friend = manager.getUserMaster(friendId);
            if (!friend.getBasePid().equals(newPartitionId) && !friend.getReplicaPids().contains(newPartitionId)) {
                replicasToAddInDestinationPartition.add(friendId);
            }
        }

//        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);
//        Set<Integer> replicasToDeleteInSourcePartition = new HashSet<>(strategy.findReplicasInMovingPartitionToDelete(repUser1, replicasToAddInDestinationPartition));
        TIntIntMap uidToPidMap = getUToMasterMap(manager.getPartitionToUserMap());
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(manager.getPartitionToReplicasMap(), new TIntHashSet(manager.getUids()));
        TIntSet replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(repUser1, new TIntHashSet(replicasToAddInDestinationPartition), minNumReplicas, uidToReplicasMap, uidToPidMap, manager.getFriendships());
        manager.moveUser(repUser1, newPartitionId, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);

        RepUser repUser1Again = manager.getUserMaster(userId1);
        TIntSet expectedReplicaIds = new TIntHashSet(oldReplicaIds);
        expectedReplicaIds.remove(newPartitionId);
        assertEquals(expectedReplicaIds, repUser1Again.getReplicaPids());
        assertEquals(newPartitionId, repUser1Again.getBasePid());

        //Check that the partitions are correct
        assertNotNull(manager.getPartitionById(newPartitionId).getMasterById(userId1));
        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            if (pid != newPartitionId) {
                assertNull(manager.getPartitionById(pid).getMasterById(userId1));
            }
        }

        //Check that the replicas are correct:
        for(TIntIterator iter = repUser1Again.getReplicaPids().iterator(); iter.hasNext(); ) {
            int partitionId = iter.next();
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


        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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
        RepUser repUser3 = manager.getUserMaster(userId3);

        //This should not be true, as it would violate our balance constraint
        assertNotEquals(repUser1.getBasePid(), repUser3.getBasePid());

        manager.befriend(repUser3, repUser1);

        assertTrue(repUser1.getFriendIDs().contains(userId3));
        assertTrue(repUser3.getFriendIDs().contains(userId1));

        for(TIntIterator iter = repUser1.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
        }

        for(TIntIterator iter = repUser3.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId3);
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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
        RepUser repUser2 = manager.getUserMaster(userId2);
        RepUser repUser3 = manager.getUserMaster(userId3);

        //This should not be true, as it would violate our balance constraint
        assertNotEquals(repUser1.getBasePid(), repUser3.getBasePid());

        manager.befriend(repUser3, repUser1);
        manager.befriend(repUser2, repUser1);

        assertTrue(repUser1.getFriendIDs().contains(userId3));
        assertTrue(repUser1.getFriendIDs().contains(userId2));
        assertTrue(repUser2.getFriendIDs().contains(userId1));
        assertTrue(repUser3.getFriendIDs().contains(userId1));

        for(TIntIterator iter = repUser1.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertTrue(replica.getFriendIDs().contains(userId2));
        }

        for(TIntIterator iter = repUser2.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId2);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        for(TIntIterator iter = repUser3.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId3);
            assertTrue(replica.getFriendIDs().contains(userId1));
        }

        manager.unfriend(repUser2, repUser1);

        assertTrue(repUser1.getFriendIDs().contains(userId3));
        assertFalse(repUser1.getFriendIDs().contains(userId2));
        assertFalse(repUser2.getFriendIDs().contains(userId1));
        assertTrue(repUser3.getFriendIDs().contains(userId1));

        for(TIntIterator iter = repUser1.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId1);
            assertTrue(replica.getFriendIDs().contains(userId3));
            assertFalse(replica.getFriendIDs().contains(userId2));
        }

        for(TIntIterator iter = repUser2.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId2);
            assertFalse(replica.getFriendIDs().contains(userId1));
        }

        for(TIntIterator iter = repUser3.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId3);
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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
        Integer user1OriginalMasterPartitionId = repUser1.getBasePid();
        TIntSet user1OriginalReplicaIds = new TIntHashSet(repUser1.getReplicaPids());
        for(TIntIterator iter = repUser1.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId1);
            assertEquals(replica.getBasePid(), user1OriginalMasterPartitionId);
            assertEquals(replica.getReplicaPids(), user1OriginalReplicaIds);
        }

        Integer user1NewMasterPartitionId = repUser1.getReplicaPids().iterator().next();
        assertNotEquals(user1NewMasterPartitionId, user1OriginalMasterPartitionId);
        manager.promoteReplicaToMaster(userId1, user1NewMasterPartitionId);

        TIntSet user1NewReplicaIds = new TIntHashSet(user1OriginalReplicaIds);
        user1NewReplicaIds.remove(user1NewMasterPartitionId);
        RepUser repUser1Again = manager.getUserMaster(userId1);
        assertEquals(repUser1Again.getBasePid(), user1NewMasterPartitionId);
        assertEquals(repUser1Again.getReplicaPids(), user1NewReplicaIds);

        for(TIntIterator iter = repUser1Again.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = manager.getPartitionById(iter.next()).getReplicaById(userId1);
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

        TIntObjectMap<User> userIdToUserMap = new TIntObjectHashMap<>();
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

        TIntSet partitionsWithoutThisUser = new TIntHashSet();
        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            RepManager.Partition partition = manager.getPartitionById(pid);
            if (!partition.getIdsOfMasters().contains(userId1) && !partition.getIdsOfReplicas().contains(userId1)) {
                partitionsWithoutThisUser.add(pid);
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
        TIntSet initialReplicaLocations = manager.getPartitionsToAddInitialReplicas(thirdPartitionId);

        assertFalse(initialReplicaLocations.contains(thirdPartitionId));
    }

    @Test
    public void testGetEdgeCut() {
        int minNumReplicas = 0;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, new TIntHashSet());
        replicaPartitions.put(2, new TIntHashSet());
        replicaPartitions.put(3, new TIntHashSet());

        RepManager manager = initRepManager(minNumReplicas+1, 0, convert(partitions), convert(friendships), convert(replicaPartitions));

        assertEquals(manager.getEdgeCut(), (Integer) 25);

        replicaPartitions.put(1, initSet(15, 16, 17, 18,  19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5,  6,  7));
        replicaPartitions.put(3, initSet( 8,  9, 10, 11, 12, 13, 14));

        manager = initRepManager(minNumReplicas, 0, convert(partitions), convert(friendships), convert(replicaPartitions));

        assertEquals(manager.getEdgeCut(), (Integer) 25);
    }


}