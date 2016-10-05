package io.vntr.spar;

import io.vntr.User;

import java.util.*;

import org.junit.Test;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.*;

public class SparMigrationStrategyTest {
    @Test
    public void testScoreReplicaPromotion() {
        SparManager manager = new SparManager(2);
        SparMigrationStrategy strategy = new SparMigrationStrategy(manager);

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

        Integer userId4 = 7;
        User user4 = new User(userId4);
        manager.addUser(user4);
        userIdToUserMap.put(userId4, user4);

        Integer userId5 = 9;
        User user5 = new User(userId5);
        manager.addUser(user5);
        userIdToUserMap.put(userId5, user5);

        SparUser sparUser1 = manager.getUserMasterById(userId1);
        SparUser sparUser3 = manager.getUserMasterById(userId3);

        manager.befriend(sparUser3, sparUser1);

        for (Integer partitionId : manager.getAllPartitionIds()) {
            assertTrue(manager.getPartitionById(partitionId).getNumMasters() == 1);
        }

        Integer userId6 = 29;
        User user6 = new User(userId6);
        manager.addUser(user6);
        userIdToUserMap.put(userId6, user6);

        SparUser sparUser6 = manager.getUserMasterById(userId6);

        outer:
        for (Integer partitionId : sparUser6.getReplicaPartitionIds()) {
            for (Integer userId : userIdToUserMap.keySet()) {
                SparUser sparUser = manager.getUserMasterById(userId);
                if (sparUser.getMasterPartitionId().equals(partitionId)) {
                    manager.befriend(sparUser, sparUser6);
                    break outer;
                }
            }
        }

        Integer friendPartitionId = manager.getUserMasterById(sparUser6.getFriendIDs().iterator().next()).getMasterPartitionId();

        assertTrue(strategy.scoreReplicaPromotion(sparUser6, friendPartitionId) == 1f);
        Set<Integer> replicaPartitionIds = new HashSet<Integer>(sparUser6.getReplicaPartitionIds());
        replicaPartitionIds.remove(friendPartitionId);
        Integer nonFriendPartitionId = replicaPartitionIds.iterator().next();
        assertTrue(strategy.scoreReplicaPromotion(sparUser6, nonFriendPartitionId) == 0f);

        Integer[] userIdArray = {3, 4, 5, 6, 8, 10, 11, 12, 13};
        for (int i = 0; i < 9; i++) {
            User user = new User(userIdArray[i]);
            manager.addUser(user);
            userIdToUserMap.put(userIdArray[i], user);
        }

        Set<SparUser> usersOnNonFriendPartition = new HashSet<SparUser>();
        for (Integer userId : userIdToUserMap.keySet()) {
            SparUser sparUser = manager.getUserMasterById(userId);
            if (sparUser.getMasterPartitionId().equals(nonFriendPartitionId)) {
                usersOnNonFriendPartition.add(sparUser);
            }
        }

        assertTrue(usersOnNonFriendPartition.size() == 3);

        for (SparUser sparUser : usersOnNonFriendPartition) {
            manager.befriend(sparUser, sparUser6);
        }

        assertTrue((strategy.scoreReplicaPromotion(sparUser6, nonFriendPartitionId)) == 2.25);
    }

    @Test
    public void testGetRemainingSpotsInPartitions() {
        SparManager manager = new SparManager(2);
        SparMigrationStrategy strategy = new SparMigrationStrategy(manager);

        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();
        manager.addPartition();

        Map<Integer, User> userIdToUserMap = new HashMap<Integer, User>();
        Integer[] userIdArray = {3, 4, 5, 6, 8, 10, 11, 12};
        for (int i = 0; i < 8; i++) {
            User user = new User(userIdArray[i]);
            manager.addUser(user);
            userIdToUserMap.put(userIdArray[i], user);
        }

        Set<Integer> partitionsWithOnlyOneMaster = new HashSet<Integer>();
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (manager.getPartitionById(partitionId).getNumMasters() == 1) {
                partitionsWithOnlyOneMaster.add(partitionId);
            }
        }

        Map<Integer, Integer> remainingSpotsInPartitions = strategy.getRemainingSpotsInPartitions(new HashSet<Integer>());
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (partitionsWithOnlyOneMaster.contains(partitionId)) {
                assertTrue(remainingSpotsInPartitions.get(partitionId).intValue() == 1);
            } else {
                assertTrue(remainingSpotsInPartitions.get(partitionId).intValue() == 0);
            }
        }

        Integer partitionIdToRob = partitionsWithOnlyOneMaster.iterator().next();
        Set<Integer> partitionsWithTwoMasters = new HashSet<Integer>(manager.getAllPartitionIds());
        partitionsWithTwoMasters.removeAll(partitionsWithOnlyOneMaster);
        Integer partitionIdToSendTo = partitionsWithTwoMasters.iterator().next();

        SparUser user = manager.getUserMasterById(manager.getPartitionById(partitionIdToRob).getIdsOfMasters().iterator().next());
        manager.moveUser(user, partitionIdToSendTo, new HashSet<Integer>(), new HashSet<Integer>());

        Map<Integer, Integer> remainingSpotsInPartitions2 = strategy.getRemainingSpotsInPartitions(new HashSet<Integer>());

        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (partitionIdToRob.equals(partitionId)) {
                assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == 2);
            } else if (partitionIdToSendTo.equals(partitionId)) {
                assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == -1);
            } else if (partitionsWithOnlyOneMaster.contains(partitionId)) {
                assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == 1);
            } else if (partitionsWithTwoMasters.contains(partitionId)) {
                assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == 0);
            }
        }

        Integer partitionIdWithTwoMasters = -1;
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (manager.getPartitionById(partitionId).getNumMasters() == 2) {
                partitionIdWithTwoMasters = partitionId;
                break;
            }
        }

        Map<Integer, Integer> remainingSpotsInPartitions3 = strategy.getRemainingSpotsInPartitions(new HashSet<Integer>(Arrays.asList(partitionIdWithTwoMasters)));
        for (Integer partitionId : manager.getAllPartitionIds()) {
            if (!partitionId.equals(partitionIdWithTwoMasters)) {
                int numMasters = manager.getPartitionById(partitionId).getNumMasters();
                int claimedRemainingSpots = remainingSpotsInPartitions3.get(partitionId);
                assertTrue(numMasters + claimedRemainingSpots == 2);
            }
        }
    }

    @Test
    public void testGetUserMigrationStrategy() {
        int minNumReplicas = 2;
        Map<Integer, Set<Integer>> partitions = new HashMap<Integer, Set<Integer>>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

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
        replicaPartitions.put(1, initSet( 6,  7,  8,  9, 10, 16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 11, 12, 13, 14, 15));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10, 16, 17, 18, 19, 20));
        replicaPartitions.put(4, initSet( 1,  2,  3,  4,  5, 11, 12, 13, 14, 15));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas+1, partitions, friendships, replicaPartitions);

        Map<Integer, Integer> expectedMigrationStrategy = new HashMap<Integer, Integer>();
        expectedMigrationStrategy.put( 6, 1);
        expectedMigrationStrategy.put( 7, 1);
        expectedMigrationStrategy.put( 8, 3);
        expectedMigrationStrategy.put( 9, 3);
        expectedMigrationStrategy.put(10, 1);
        SparMigrationStrategy strategy = new SparMigrationStrategy(manager);
        Map<Integer, Integer> migrationStrategy = strategy.getUserMigrationStrategy(2);
        assertEquals(expectedMigrationStrategy, migrationStrategy);


    }
}