package io.vntr.utils;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.manager.RepManager;
import org.junit.Test;

import static io.vntr.utils.InitUtils.initNoRepManager;
import static io.vntr.utils.InitUtils.initRepManager;
import static io.vntr.utils.TroveUtils.initSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class InitUtilsTest {

    @Test
    public void testInitRepManager() {
        short minNumReplicas = 0;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put((short)2, initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put((short)3, initSet(15, 16, 17, 18,  19, 20));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short) 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put((short) 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put((short) 3, initSet( 4,  8, 12, 16, 20));
        friendships.put((short) 4, initSet( 5, 10, 15));
        friendships.put((short) 5, initSet( 6, 12, 18));
        friendships.put((short) 6, initSet( 7, 14));
        friendships.put((short) 7, initSet( 8, 16));
        friendships.put((short) 8, initSet( 9, 18));
        friendships.put((short) 9, initSet(10, 20));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, initSet(14));
        friendships.put((short)14, initSet(15));
        friendships.put((short)15, initSet(16));
        friendships.put((short)16, initSet(17));
        friendships.put((short)17, initSet(18));
        friendships.put((short)18, initSet(19));
        friendships.put((short)19, initSet(20));
        friendships.put((short)20, new TShortHashSet());

        TShortObjectMap<TShortSet> replicaPartitions = new TShortObjectHashMap<>();
        replicaPartitions.put((short)1, new TShortHashSet());
        replicaPartitions.put((short)2, new TShortHashSet());
        replicaPartitions.put((short)3, new TShortHashSet());

        TShortObjectMap<TShortSet> bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(friendships);

        RepManager manager = initRepManager((short)(minNumReplicas+1), 0, partitions, friendships, replicaPartitions);

        assertEquals(manager.getEdgeCut(), (Integer) 25);

        replicaPartitions.put((short)1, initSet(15, 16, 17, 18,  19, 20));
        replicaPartitions.put((short)2, initSet( 1,  2,  3,  4,  5,  6,  7));
        replicaPartitions.put((short)3, initSet( 8,  9, 10, 11, 12, 13, 14));


        manager = initRepManager(minNumReplicas, 0, partitions, friendships, replicaPartitions);


        assertEquals(manager.getEdgeCut(), (Integer) 25);
        assertEquals(manager.getFriendships(), bidirectionalFriendships);
        assertEquals(manager.getPartitionToUserMap(), partitions);
        assertEquals(manager.getPartitionToReplicasMap(), replicaPartitions);
        assertTrue(manager.getNumUsers() == friendships.size());
        assertEquals(new TShortHashSet(manager.getPids()), partitions.keySet());
        assertEquals(new TShortHashSet(manager.getUids()), friendships.keySet());


        int replicationCount = 0;
        for(short replicaPid : replicaPartitions.keys()) {
            replicationCount += replicaPartitions.get(replicaPid).size();
        }
        assertTrue(manager.getReplicationCount() == replicationCount);

        for(short pid : partitions.keys()) {
            assertEquals(partitions.get(pid), new TShortHashSet(manager.getMastersOnPartition(pid)));
        }

        for(short pid : replicaPartitions.keys()) {
            assertEquals(replicaPartitions.get(pid), new TShortHashSet(manager.getReplicasOnPartition(pid)));
        }

        TShortShortMap uidToPidMap = TroveUtils.getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToReplicasMap = TroveUtils.getUToReplicasMap(replicaPartitions, friendships.keySet());

        for(short uid : friendships.keys()) {
            RepUser user = manager.getUserMaster(uid);
            assertTrue(user.getId() == uid);
            assertTrue(user.getBasePid() == uidToPidMap.get(uid));
            assertEquals(user.getReplicaPids(), uidToReplicasMap.get(uid));
            assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
            for(TShortIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
                RepUser replica = manager.getReplicaOnPartition(uid, iter.next());
                assertEquals(replica, user);
            }
        }
    }

    @Test
    public void testInitNoRepManager() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2, initSet( 6,  7,  8,  9));
        partitions.put((short)3, initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        TShortObjectMap<TShortSet> bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(friendships);

        int expectedCut = 56; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3

        NoRepManager manager = initNoRepManager(0, true, partitions, friendships);
        assertEquals(new TShortHashSet(manager.getPids()), partitions.keySet());
        assertEquals(partitions, manager.getPartitionToUsers());
        assertEquals(bidirectionalFriendships, manager.getFriendships());
        assertTrue(manager.getMigrationTally() == 0);

        for(short pid : partitions.keys()) {
            assertEquals(partitions.get(pid), new TShortHashSet(manager.getPartition(pid)));
            for(TShortIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
                short uid = iter.next();
                User user = manager.getUser(uid);
                assertEquals(user.getBasePid(), pid);
                assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
                assertTrue(uid == user.getId());
            }
        }

        assertTrue(manager.getEdgeCut() == expectedCut);
    }

}
