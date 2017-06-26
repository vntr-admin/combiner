package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
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
        short minNumReplicas = 1;
        float gamma = 1.5f;

        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put((short)2,  initSet(3, 6, 9, 12));
        friendships.put((short)3,  initSet(4, 8, 12));
        friendships.put((short)4,  initSet(5, 10));
        friendships.put((short)5,  initSet(6, 12));
        friendships.put((short)6,  initSet(7));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  6, 8, 9));

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        Set<Target> targets = SparmesRepartitioner.getPartitionCandidates((short)2, true, (short)3, state);
        assertEquals(targets, TestUtils.initSet(new Target((short)9, (short)3, (short)2, 1f)));

        targets = SparmesRepartitioner.getPartitionCandidates((short)2, false, (short)3, state);
        assertEquals(targets, TestUtils.initSet(new Target((short)6, (short)1, (short)2, 1f), new Target((short)8, (short)1, (short)2, 1f), new Target((short)9, (short)1, (short)2, 1f)));
    }

    @Test
    public void testGetPartitionCandidatesSecondStage() {
        short minNumReplicas = 1;
        float gamma = 1.5f;

        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put((short)2,  initSet(3, 6, 9, 12));
        friendships.put((short)3,  initSet(4, 8, 12));
        friendships.put((short)4,  initSet(5, 10));
        friendships.put((short)5,  initSet(6, 12));
        friendships.put((short)6,  initSet(7));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  9));

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        Set<Target> targets = SparmesRepartitioner.getPartitionCandidates((short)2, false, (short)3, state);
        assertEquals(targets, TestUtils.initSet(new Target((short)9, (short)1, (short)2, 1f)));
    }

    @Test
    public void testMigrateLogicallyAndUpdateLogicalUsers() {
        short minNumReplicas = 1;
        float gamma = 1.5f;

        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put((short)2,  initSet(3, 6, 9, 12));
        friendships.put((short)3,  initSet(4, 8, 12));
        friendships.put((short)4,  initSet(5, 10));
        friendships.put((short)5,  initSet(6, 12));
        friendships.put((short)6,  initSet(7));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  9));

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        short uid1 = 1;
        short pid1 = 1;

        short uid2 = 13;
        short pid2 = 3;

        assertTrue(state.getLogicalUsers().get(uid1).getPid() == pid1);
        assertTrue(state.getLogicalUsers().get(uid2).getPid() == pid2);
        assertTrue(state.getLogicalPartitions().get(pid1).contains(uid1));
        assertTrue(state.getLogicalPartitions().get(pid2).contains(uid2));
        assertEquals(state.getLogicalReplicaPartitions().get(pid1), replicas.get(pid1));
        assertEquals(state.getLogicalReplicaPartitions().get(pid2), replicas.get(pid2));

        Set<Target> targets = TestUtils.initSet(new Target((short)1, (short)3, (short)1, 2f), new Target((short)13, (short)1, (short)3, 2f));
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
        short minNumReplicas = 1;
        float gamma = 1.5f;

        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put((short)2,  initSet(3, 6, 9, 12));
        friendships.put((short)3,  initSet(4, 8, 12));
        friendships.put((short)4,  initSet(5, 10));
        friendships.put((short)5,  initSet(6, 12));
        friendships.put((short)6,  initSet(7));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  9));

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        SparmesRepartitioner.State state = SparmesRepartitioner.State.init(minNumReplicas, gamma, partitions, replicas, bidirectionalFriendships);

        assertEquals(state.getLogicalPartitions(), partitions);
        assertEquals(state.getLogicalReplicaPartitions(), replicas);
        assertEquals(state.getLogicalUsers().keySet(), friendships.keySet());

        for(TShortIterator iter = friendships.keySet().iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            SparmesRepartitioner.LogicalUser user = state.getLogicalUsers().get(uid);
            assertTrue(user.getId() == uid);
            assertTrue(user.getPid() == uidToPidMap.get(uid));
            assertEquals(user.getReplicaLocations(), uidToReplicasMap.get(uid));
            assertEquals(user.getFriendIds(), bidirectionalFriendships.get(uid));
            assertTrue(user.getMinNumReplicas() == minNumReplicas);
            assertTrue(user.getTotalWeight() == friendships.size());

            boolean shouldReplicateInSourcePartition = !disjoint(bidirectionalFriendships.get(uid), partitions.get(uidToPidMap.get(uid)));
            assertTrue(shouldReplicateInSourcePartition == user.isReplicateInSourcePartition());

            TShortShortMap pToWeight = user.getpToWeight();
            for(TShortIterator iter2 = partitions.keySet().iterator(); iter2.hasNext(); ) {
                short pid = iter2.next();
                assertEquals(pToWeight.get(pid), partitions.get(pid).size());
            }

            TShortShortMap pToFriendCount = user.getpToFriendCount();
            for(TShortIterator iter2 = partitions.keySet().iterator(); iter2.hasNext(); ) {
                short pid = iter2.next();
                TShortSet friends = new TShortHashSet(bidirectionalFriendships.get(uid));
                friends.retainAll(partitions.get(pid));
                assertTrue(pToFriendCount.get(pid) == friends.size());
            }

            TShortShortMap numFriendsToAddInEachPartition = user.getNumFriendsToAddInEachPartition();
            for(TShortIterator iter2 = partitions.keySet().iterator(); iter2.hasNext(); ) {
                short pid = iter2.next();
                if(pid != user.getPid()) {
                    TShortSet friends = new TShortHashSet(bidirectionalFriendships.get(uid));
                    friends.removeAll(partitions.get(pid));
                    friends.removeAll(replicas.get(pid));
                    assertTrue(numFriendsToAddInEachPartition.get(pid) == friends.size());
                }
            }

            TShortSet mastersOnPartition = partitions.get(uidToPidMap.get(uid));
            TShortSet sufficientlyReplicatedFriends = getFriendsExceedingKReplication(uid, minNumReplicas, bidirectionalFriendships, uidToReplicasMap);
            sufficientlyReplicatedFriends.removeAll(mastersOnPartition);
            short count = 0;
            for(TShortIterator iter2 = sufficientlyReplicatedFriends.iterator(); iter2.hasNext(); ) {
                short friendId = iter2.next();
                TShortSet friendsOfFriend = new TShortHashSet(bidirectionalFriendships.get(friendId));
                friendsOfFriend.remove(uid);
                if(disjoint(friendsOfFriend, mastersOnPartition)) {
                    count++;
                }
            }

            assertTrue(user.getNumFriendReplicasToDeleteInSourcePartition() == count);
        }
    }

    private static TShortSet getFriendsExceedingKReplication(short uid, short minNumReplicas, TShortObjectMap<TShortSet> bidirectionalFriendships, TShortObjectMap<TShortSet> uidToReplicasMap) {
        TShortSet friends = new TShortHashSet(bidirectionalFriendships.get(uid));
        for(TShortIterator iter = friends.iterator(); iter.hasNext(); ) {
            if(uidToReplicasMap.get(iter.next()).size() <= minNumReplicas) {
                iter.remove();
            }
        }
        return friends;
    }
}
