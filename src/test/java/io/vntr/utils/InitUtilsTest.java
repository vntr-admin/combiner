package io.vntr.utils;

import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.manager.RepManager;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.InitUtils.initNoRepManager;
import static io.vntr.utils.InitUtils.initRepManager;
import static io.vntr.utils.Utils.generateBidirectionalFriendshipSet;
import static io.vntr.utils.Utils.getUToMasterMap;
import static io.vntr.utils.Utils.getUToReplicasMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class InitUtilsTest {

    @Test
    public void testInitRepManager() {
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

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        RepManager manager = initRepManager(minNumReplicas+1, 0, partitions, friendships, replicaPartitions);

        assertEquals(manager.getEdgeCut(), (Integer) 25);

        replicaPartitions.put(1, initSet(15, 16, 17, 18,  19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5,  6,  7));
        replicaPartitions.put(3, initSet( 8,  9, 10, 11, 12, 13, 14));


        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);


        assertEquals(manager.getEdgeCut(), (Integer) 25);
        assertEquals(manager.getFriendships(), bidirectionalFriendships);
        assertEquals(manager.getPartitionToUserMap(), partitions);
        assertEquals(manager.getPartitionToReplicasMap(), replicaPartitions);
        assertTrue(manager.getNumUsers() == friendships.size());
        assertEquals(manager.getPids(), partitions.keySet());
        assertEquals(manager.getUids(), friendships.keySet());


        int replicationCount = 0;
        for(Set<Integer> replicas : replicaPartitions.values()) {
            replicationCount += replicas.size();
        }
        assertTrue(manager.getReplicationCount() == replicationCount);

        for(int pid : partitions.keySet()) {
            assertEquals(partitions.get(pid), manager.getMastersOnPartition(pid));
        }

        for(int pid : replicaPartitions.keySet()) {
            assertEquals(replicaPartitions.get(pid), manager.getReplicasOnPartition(pid));
        }

        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(replicaPartitions, friendships.keySet());

        for(int uid : friendships.keySet()) {
            RepUser user = manager.getUserMaster(uid);
            assertTrue(user.getId() == uid);
            assertEquals(user.getBasePid(), uidToPidMap.get(uid));
            assertEquals(user.getReplicaPids(), uidToReplicasMap.get(uid));
            assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
            for(int pid : user.getReplicaPids()) {
                RepUser replica = manager.getReplicaOnPartition(uid, pid);
                assertEquals(replica, user);
            }
        }
    }

    @Test
    public void testInitNoRepManager() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        int expectedCut = 56; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3

        NoRepManager manager = initNoRepManager(0, true, partitions, friendships);
        assertEquals(manager.getPids(), partitions.keySet());
        assertEquals(partitions, manager.getPartitionToUsers());
        assertEquals(bidirectionalFriendships, manager.getFriendships());
        assertTrue(manager.getMigrationTally() == 0);

        for(Integer pid : partitions.keySet()) {
            assertEquals(partitions.get(pid), manager.getPartition(pid));
            for(Integer uid : partitions.get(pid)) {
                User user = manager.getUser(uid);
                assertEquals(user.getBasePid(), pid);
                assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
                assertEquals(uid, user.getId());
            }
        }

        assertTrue(manager.getEdgeCut() == expectedCut);
    }

}
