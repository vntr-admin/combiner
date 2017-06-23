package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.TestUtils;
import org.junit.Test;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class SparmesRepartitionerTest {

    @Test
    public void testGetPartitionCandidatesFirstStage() {
        int minNumReplicas = 1;
        float gamma = 1.5f;

        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        Set<Target> targets = SparmesRepartitioner.getPartitionCandidates(2, true, 3, state);
        assertEquals(targets, TestUtils.initSet(new Target(9, 3, 2, 1f)));

        targets = SparmesRepartitioner.getPartitionCandidates(2, false, 3, state);
        assertEquals(targets, TestUtils.initSet(new Target(6, 1, 2, 1f), new Target(8, 1, 2, 1f), new Target(9, 1, 2, 1f)));
    }

    @Test
    public void testGetPartitionCandidatesSecondStage() {
        int minNumReplicas = 1;
        float gamma = 1.5f;

        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        Set<Target> targets = SparmesRepartitioner.getPartitionCandidates(2, false, 3, state);
        assertEquals(targets, TestUtils.initSet(new Target(9, 1, 2, 1f)));
    }

    @Test
    public void testMigrateLogicallyAndUpdateLogicalUsers() {
        int minNumReplicas = 1;
        float gamma = 1.5f;

        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        int uid1 = 1;
        int pid1 = 1;

        int uid2 = 13;
        int pid2 = 3;

        assertTrue(state.getLogicalUsers().get(uid1).getPid() == pid1);
        assertTrue(state.getLogicalUsers().get(uid2).getPid() == pid2);
        assertTrue(state.getLogicalPartitions().get(pid1).contains(uid1));
        assertTrue(state.getLogicalPartitions().get(pid2).contains(uid2));
        assertEquals(state.getLogicalReplicaPartitions().get(pid1), replicas.get(pid1));
        assertEquals(state.getLogicalReplicaPartitions().get(pid2), replicas.get(pid2));

        Set<Target> targets = TestUtils.initSet(new Target(1, 3, 1, 2f), new Target(13, 1, 3, 2f));
        for(Target target : targets) {
            SparmesRepartitioner.migrateLogically(target, state, uidToReplicasMap);
        }
        state.updateLogicalUsers();

        assertTrue(state.getLogicalUsers().get(uid1).getPid() == pid2);
        assertTrue(state.getLogicalUsers().get(uid2).getPid() == pid1);
        assertTrue(state.getLogicalPartitions().get(pid1).contains(uid2));
        assertTrue(state.getLogicalPartitions().get(pid2).contains(uid1));
        assertEquals(state.getLogicalReplicaPartitions().get(pid1), initSet( 1,  6,  7,  8,  9, 10, 12));
        assertEquals(state.getLogicalReplicaPartitions().get(pid2), initSet( 2,  3,  4,  5,  6,  8,  9, 13));
    }

    @Test
    public void testStateDotInit() {
        int minNumReplicas = 1;
        float gamma = 1.5f;

        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
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
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        assertEquals(state.getLogicalPartitions(), partitions);
        assertEquals(state.getLogicalReplicaPartitions(), replicas);
        assertEquals(state.getLogicalUsers().keySet(), friendships.keySet());

        for(TIntIterator iter = friendships.keySet().iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            SparmesRepartitioner.LogicalUser user = state.getLogicalUsers().get(uid);
            assertTrue(user.getId() == uid);
            assertTrue(user.getPid() == uidToPidMap.get(uid));
            assertEquals(user.getReplicaLocations(), uidToReplicasMap.get(uid));
            assertEquals(user.getFriendIds(), bidirectionalFriendships.get(uid));
            assertTrue(user.getMinNumReplicas() == minNumReplicas);
            assertTrue(user.getTotalWeight() == friendships.size());

            boolean shouldReplicateInSourcePartition = !disjoint(bidirectionalFriendships.get(uid), partitions.get(uidToPidMap.get(uid)));
            assertTrue(shouldReplicateInSourcePartition == user.isReplicateInSourcePartition());

            TIntIntMap pToWeight = user.getpToWeight();
            for(TIntIterator iter2 = partitions.keySet().iterator(); iter2.hasNext(); ) {
                int pid = iter2.next();
                assertEquals(pToWeight.get(pid), partitions.get(pid).size());
            }

            TIntIntMap pToFriendCount = user.getpToFriendCount();
            for(TIntIterator iter2 = partitions.keySet().iterator(); iter2.hasNext(); ) {
                int pid = iter2.next();
                TIntSet friends = new TIntHashSet(bidirectionalFriendships.get(uid));
                friends.retainAll(partitions.get(pid));
                assertTrue(pToFriendCount.get(pid) == friends.size());
            }

            TIntIntMap numFriendsToAddInEachPartition = user.getNumFriendsToAddInEachPartition();
            for(TIntIterator iter2 = partitions.keySet().iterator(); iter2.hasNext(); ) {
                int pid = iter2.next();
                if(pid != user.getPid()) {
                    TIntSet friends = new TIntHashSet(bidirectionalFriendships.get(uid));
                    friends.removeAll(partitions.get(pid));
                    friends.removeAll(replicas.get(pid));
                    assertTrue(numFriendsToAddInEachPartition.get(pid) == friends.size());
                }
            }

            TIntSet mastersOnPartition = partitions.get(uidToPidMap.get(uid));
            TIntSet sufficientlyReplicatedFriends = getFriendsExceedingKReplication(uid, minNumReplicas, bidirectionalFriendships, uidToReplicasMap);
            sufficientlyReplicatedFriends.removeAll(mastersOnPartition);
            int count = 0;
            for(TIntIterator iter2 = sufficientlyReplicatedFriends.iterator(); iter2.hasNext(); ) {
                int friendId = iter2.next();
                TIntSet friendsOfFriend = new TIntHashSet(bidirectionalFriendships.get(friendId));
                friendsOfFriend.remove(uid);
                if(disjoint(friendsOfFriend, mastersOnPartition)) {
                    count++;
                }
            }

            assertTrue(user.getNumFriendReplicasToDeleteInSourcePartition() == count);
        }
    }

    private static TIntSet getFriendsExceedingKReplication(int uid, int minNumReplicas, TIntObjectMap<TIntSet> bidirectionalFriendships, TIntObjectMap<TIntSet> uidToReplicasMap) {
        TIntSet friends = new TIntHashSet(bidirectionalFriendships.get(uid));
        for(TIntIterator iter = friends.iterator(); iter.hasNext(); ) {
            if(uidToReplicasMap.get(iter.next()).size() <= minNumReplicas) {
                iter.remove();
            }
        }
        return friends;
    }
}
