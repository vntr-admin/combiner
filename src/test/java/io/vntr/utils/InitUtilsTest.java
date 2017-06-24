package io.vntr.utils;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.manager.RepManager;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.InitUtils.initNoRepManager;
import static io.vntr.utils.InitUtils.initRepManager;
import static io.vntr.utils.TroveUtils.convert;
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
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4,  5,  6,  7));
        partitions.put(2, TroveUtils.initSet( 8,  9, 10, 11, 12, 13, 14));
        partitions.put(3, TroveUtils.initSet(15, 16, 17, 18,  19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put( 1, TroveUtils.initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, TroveUtils.initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, TroveUtils.initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, TroveUtils.initSet( 5, 10, 15));
        friendships.put( 5, TroveUtils.initSet( 6, 12, 18));
        friendships.put( 6, TroveUtils.initSet( 7, 14));
        friendships.put( 7, TroveUtils.initSet( 8, 16));
        friendships.put( 8, TroveUtils.initSet( 9, 18));
        friendships.put( 9, TroveUtils.initSet(10, 20));
        friendships.put(10, TroveUtils.initSet(11));
        friendships.put(11, TroveUtils.initSet(12));
        friendships.put(12, TroveUtils.initSet(13));
        friendships.put(13, TroveUtils.initSet(14));
        friendships.put(14, TroveUtils.initSet(15));
        friendships.put(15, TroveUtils.initSet(16));
        friendships.put(16, TroveUtils.initSet(17));
        friendships.put(17, TroveUtils.initSet(18));
        friendships.put(18, TroveUtils.initSet(19));
        friendships.put(19, TroveUtils.initSet(20));
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, new TIntHashSet());
        replicaPartitions.put(2, new TIntHashSet());
        replicaPartitions.put(3, new TIntHashSet());

        TIntObjectMap<TIntSet> bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(friendships);

        RepManager manager = initRepManager(minNumReplicas+1, 0, convert(partitions), convert(friendships), convert(replicaPartitions));

        assertEquals(manager.getEdgeCut(), (Integer) 25);

        replicaPartitions.put(1, TroveUtils.initSet(15, 16, 17, 18,  19, 20));
        replicaPartitions.put(2, TroveUtils.initSet( 1,  2,  3,  4,  5,  6,  7));
        replicaPartitions.put(3, TroveUtils.initSet( 8,  9, 10, 11, 12, 13, 14));


        manager = initRepManager(minNumReplicas, 0, convert(partitions), convert(friendships), convert(replicaPartitions));


        assertEquals(manager.getEdgeCut(), (Integer) 25);
        assertEquals(manager.getFriendships(), bidirectionalFriendships);
        assertEquals(manager.getPartitionToUserMap(), partitions);
        assertEquals(manager.getPartitionToReplicasMap(), replicaPartitions);
        assertTrue(manager.getNumUsers() == friendships.size());
        assertEquals(new TIntHashSet(manager.getPids()), partitions.keySet());
        assertEquals(new TIntHashSet(manager.getUids()), friendships.keySet());


        int replicationCount = 0;
        for(int replicaPid : replicaPartitions.keys()) {
            replicationCount += replicaPartitions.get(replicaPid).size();
        }
        assertTrue(manager.getReplicationCount() == replicationCount);

        for(int pid : partitions.keys()) {
            assertEquals(partitions.get(pid), new TIntHashSet(manager.getMastersOnPartition(pid)));
        }

        for(int pid : replicaPartitions.keys()) {
            assertEquals(replicaPartitions.get(pid), new TIntHashSet(manager.getReplicasOnPartition(pid)));
        }

        TIntIntMap uidToPidMap = TroveUtils.getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = TroveUtils.getUToReplicasMap(replicaPartitions, friendships.keySet());

        for(int uid : friendships.keys()) {
            RepUser user = manager.getUserMaster(uid);
            assertTrue(user.getId() == uid);
            assertTrue(user.getBasePid() == uidToPidMap.get(uid));
            assertEquals(user.getReplicaPids(), uidToReplicasMap.get(uid));
            assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
            for(TIntIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
                RepUser replica = manager.getReplicaOnPartition(uid, iter.next());
                assertEquals(replica, user);
            }
        }
    }

    @Test
    public void testInitNoRepManager() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4, 5));
        partitions.put(2, TroveUtils.initSet( 6,  7,  8,  9));
        partitions.put(3, TroveUtils.initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        TIntObjectMap<TIntSet> bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(friendships);

        int expectedCut = 56; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3

        NoRepManager manager = initNoRepManager(0, true, convert(partitions), convert(friendships));
        assertEquals(new TIntHashSet(manager.getPids()), partitions.keySet());
        assertEquals(partitions, manager.getPartitionToUsers());
        assertEquals(bidirectionalFriendships, manager.getFriendships());
        assertTrue(manager.getMigrationTally() == 0);

        for(Integer pid : partitions.keys()) {
            assertEquals(partitions.get(pid), new TIntHashSet(manager.getPartition(pid)));
            for(TIntIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
                int uid = iter.next();
                User user = manager.getUser(uid);
                assertEquals(user.getBasePid(), pid);
                assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
                assertTrue(uid == user.getId());
            }
        }

        assertTrue(manager.getEdgeCut() == expectedCut);
    }

}
