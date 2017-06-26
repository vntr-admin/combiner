package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.manager.RepManager;
import io.vntr.utils.InitUtils;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static io.vntr.repartition.SpajaRepartitioner.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/18/17.
 */
public class SpajaRepartitionerTest {

    @Test
    public void testGetLogicalReplicationCount() {
        short minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        short k = 7;

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

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        short expectedReplication = (short)(replicas.get((short) 1).size() + replicas.get((short) 2).size() + replicas.get((short) 3).size());
        assertTrue(expectedReplication == SpajaRepartitioner.getLogicalReplicationCount(state.getLogicalReplicaPartitions()));
    }

    @Test
    public void testFindPartner() {
        short minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        short k = 7;

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
        friendships.put((short)11, initSet(12, 13));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  9));

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        short uid1 = 4;
        short expectedPartner = 12;
        TShortSet candidates = initSet(6, 7, 8, 9, 11, 12, 13); //omit friends and things on same partition

        Short partnerId = SpajaRepartitioner.findPartner(uid1, candidates, 2f, state);
        assertEquals(partnerId, (Short) expectedPartner);


        //Tricky...
        short trickyId1 = 1;
        short trickyPid1 = 1;
        short trickyId2 = 9;
        short trickyPid2 = 2;

        double currentReplicas = replicas.get(trickyPid1).size() + replicas.get(trickyPid2).size();
        double expectedIncreaseInReplicas = 2;

        double pivot = (currentReplicas + expectedIncreaseInReplicas) / currentReplicas;
        double differential = 0.00001;

        partnerId = SpajaRepartitioner.findPartner(trickyId1, initSet(trickyId2), (float)(pivot - differential), state);
        assertTrue(partnerId == null);

        partnerId = SpajaRepartitioner.findPartner(trickyId1, initSet(trickyId2), (float)(pivot + differential), state);
        assertEquals(partnerId, (Short)trickyId2);
    }

    @Test
    public void testGetSwapChangesThenSwap() {
        short minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        short k = 7;

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

        RepManager manager = InitUtils.initRepManager(minNumReplicas, 0, partitions, friendships, replicas);
        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);


        //Find swap changes
        short uid1 = 1;
        short pid1 = 1;
        short uid2 = 6;
        short pid2 = 2;
        SpajaRepartitioner.SwapChanges swapChanges = SpajaRepartitioner.getSwapChanges(uid1, uid2, state);

        TShortSet expectedAddToP1 = initSet(1); //1 has friends on P1, so it needs a replica there
        TShortSet expectedAddToP2 = initSet(4, 6, 12); //6 has friends on P1, and 1 is friends with 4 and 12 (which aren't there yet)
        TShortSet expectedRemoveFromP1 = initSet(6); //we're moving 6 there, so no need for a replica
        TShortSet expectedRemoveFromP2 = initSet(1, 5); //1 will be a master, so no replica, and 5 was only there for 6

        assertEquals(swapChanges.getPid1(), pid1);
        assertEquals(swapChanges.getPid2(), pid2);
        assertEquals(swapChanges.getAddToP1(), expectedAddToP1);
        assertEquals(swapChanges.getAddToP2(), expectedAddToP2);
        assertEquals(swapChanges.getRemoveFromP1(), expectedRemoveFromP1);
        assertEquals(swapChanges.getRemoveFromP2(), expectedRemoveFromP2);


        //Swap
        SpajaRepartitioner.swap(uid1, uid2, state);

        assertTrue(state.getLogicalPids().get(uid1) == pid2);
        assertTrue(state.getLogicalPids().get(uid2) == pid1);

        for(TShortIterator iter = swapChanges.getAddToP1().iterator(); iter.hasNext(); ) {
            assertTrue(state.getLogicalReplicaPids().get(iter.next()).contains(pid1));
        }
        for(TShortIterator iter = swapChanges.getAddToP2().iterator(); iter.hasNext(); ) {
            assertTrue(state.getLogicalReplicaPids().get(iter.next()).contains(pid2));
        }
        for(TShortIterator iter = swapChanges.getRemoveFromP1().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPids().get(iter.next()).contains(pid1));
        }
        for(TShortIterator iter = swapChanges.getRemoveFromP2().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPids().get(iter.next()).contains(pid2));
        }

        assertTrue(state.getLogicalReplicaPartitions().get(pid1).containsAll(swapChanges.getAddToP1()));
        assertTrue(state.getLogicalReplicaPartitions().get(pid2).containsAll(swapChanges.getAddToP2()));

        for(TShortIterator iter = swapChanges.getRemoveFromP1().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPartitions().get(pid1).contains(iter.next()));
        }
        for(TShortIterator iter = swapChanges.getRemoveFromP2().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPartitions().get(pid2).contains(iter.next()));
        }
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        short minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        short k = 7;

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

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //For every friend of the moving user:
        //    We should add a replica in the target partition if there isn't already a replica or master in that partition
        assertEquals(findReplicasToAddToTargetPartition((short)1, (short)2, state), initSet(4, 12));
        assertEquals(findReplicasToAddToTargetPartition((short)1, (short)3, state), initSet(6, 8));
        assertEquals(findReplicasToAddToTargetPartition((short)13, (short)1, state), new TShortHashSet());
    }

    @Test
    public void testFindReplicasInMovingPartitionToDelete() {
        short minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        short k = 7;

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
        friendships.put((short)11, initSet(9, 12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>();
        replicas.put((short)1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put((short)2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put((short)3, initSet( 1, 2, 3,  4,  5,  9));

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //For every friend of the moving user, if:
        //    (1) friend has a replica on movingPartition
        //    (2) friend doesn't have any other friends on movingPartition
        //    (3) friend has more than minNumReplicas replicas
        //then delete it

        //All of its friends either have masters in P1, or have replicas in P1
        TShortSet toDeleteIn1For1 = findReplicasInMovingPartitionToDelete((short)1, (short)1, findReplicasToAddToTargetPartition((short)1, (short)2, state), state);
        assertEquals(toDeleteIn1For1, new TShortHashSet());

        //10 was only in P2 for 9's sake
        TShortSet toDeleteIn2For9 = findReplicasInMovingPartitionToDelete((short)9, (short)2, findReplicasToAddToTargetPartition((short)9, (short)3, state), state);
        assertEquals(toDeleteIn2For9, initSet(10));

        //10's friends are 1, 4, 9, and 11.  1 and 9 have other friends on P3.  11 is on P3.  4's only there for 10, but would violate minNumReplicas
        TShortSet toDeleteIn3For10 = findReplicasInMovingPartitionToDelete((short)10, (short)3, findReplicasToAddToTargetPartition((short)10, (short)1, state), state);
        assertEquals(toDeleteIn3For10, new TShortHashSet());
    }

    @Test
    public void testShouldDeleteReplicaInTargetPartitionAndShouldWeAddAReplicaOfMovingUserInMovingPartition() {
        short minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        short k = 7;

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

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //Test shouldDeleteReplicaInTargetPartition
        //If there's a replica of moving user in targetPartition, then delete it
        for(short pid : replicas.keys()) {
            for(short uid : friendships.keys()) {
                if(partitions.get(pid).contains(uid)) {
                    continue;
                }
                if(replicas.get(pid).contains(uid)) {
                    assertTrue(shouldDeleteReplicaInTargetPartition(uid, pid, state));
                }
                else {
                    assertFalse(shouldDeleteReplicaInTargetPartition(uid, pid, state));
                }
            }
        }

        //Test shouldWeAddAReplicaOfMovingUserInMovingPartition
        //We should add a replica if moving user has friends in moving partition
        //Additionally, we should add one if moving user has minNumReplica replicas, including one in targetPartition
        shouldWeAddAReplicaOfMovingUserInMovingPartition((short)1, (short)1, state);
        for(short pid : replicas.keys()) {
uidLoop:    for (short uid : friendships.keys()) {
                short logicalPid = state.getLogicalPids().get(uid);
                if(partitions.get(pid).contains(uid)) {
                    continue;
                }
                if(state.getLogicalReplicaPids().get(uid).contains(uid) && state.getLogicalReplicaPids().get(uid).size() <= state.getMinNumReplicas()) {
                    assertTrue(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
                    continue;
                }
                for(TShortIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
                    short friendId = iter.next();
                    if(state.getLogicalPids().get(friendId) == logicalPid) {
                        assertTrue(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
                        continue uidLoop;
                    }
                }
                assertFalse(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
            }
        }
    }


    private static void fillState(SpajaRepartitioner.State state, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas) {
        state.setLogicalPids(getUToMasterMap(partitions));
        state.setLogicalReplicaPids(getUToReplicasMap(replicas, state.getFriendships().keySet()));
        state.setLogicalReplicaPartitions(replicas);
    }
}
