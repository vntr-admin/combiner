package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
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
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        int expectedReplication = replicas.get(1).size() + replicas.get(2).size() + replicas.get(3).size();
        assertTrue(expectedReplication == SpajaRepartitioner.getLogicalReplicationCount(state.getLogicalReplicaPartitions()));
    }

    @Test
    public void testFindPartner() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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
        friendships.put(11, initSet(12, 13));
        friendships.put(12, initSet(13));
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        Integer uid1 = 4;
        Integer expectedPartner = 12;
        TIntSet candidates = initSet(6, 7, 8, 9, 11, 12, 13); //omit friends and things on same partition

        Integer partnerId = SpajaRepartitioner.findPartner(uid1, candidates, 2f, state);
        assertEquals(partnerId, expectedPartner);


        //Tricky...
        Integer trickyId1 = 1;
        Integer trickyPid1 = 1;
        Integer trickyId2 = 9;
        Integer trickyPid2 = 2;

        double currentReplicas = replicas.get(trickyPid1).size() + replicas.get(trickyPid2).size();
        double expectedIncreaseInReplicas = 2;

        double pivot = (currentReplicas + expectedIncreaseInReplicas) / currentReplicas;
        double differential = 0.00001;

        partnerId = SpajaRepartitioner.findPartner(trickyId1, initSet(trickyId2), (float)(pivot - differential), state);
        assertTrue(partnerId == null);

        partnerId = SpajaRepartitioner.findPartner(trickyId1, initSet(trickyId2), (float)(pivot + differential), state);
        assertEquals(partnerId, trickyId2);
    }

    @Test
    public void testGetSwapChangesThenSwap() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        RepManager manager = InitUtils.initRepManager(minNumReplicas, 0, convert(partitions), convert(friendships), convert(replicas));
        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);


        //Find swap changes
        Integer uid1 = 1;
        Integer pid1 = 1;
        Integer uid2 = 6;
        Integer pid2 = 2;
        SpajaRepartitioner.SwapChanges swapChanges = SpajaRepartitioner.getSwapChanges(uid1, uid2, state);

        TIntSet expectedAddToP1 = initSet(1); //1 has friends on P1, so it needs a replica there
        TIntSet expectedAddToP2 = initSet(4, 6, 12); //6 has friends on P1, and 1 is friends with 4 and 12 (which aren't there yet)
        TIntSet expectedRemoveFromP1 = initSet(6); //we're moving 6 there, so no need for a replica
        TIntSet expectedRemoveFromP2 = initSet(1, 5); //1 will be a master, so no replica, and 5 was only there for 6

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

        for(TIntIterator iter = swapChanges.getAddToP1().iterator(); iter.hasNext(); ) {
            assertTrue(state.getLogicalReplicaPids().get(iter.next()).contains(pid1));
        }
        for(TIntIterator iter = swapChanges.getAddToP2().iterator(); iter.hasNext(); ) {
            assertTrue(state.getLogicalReplicaPids().get(iter.next()).contains(pid2));
        }
        for(TIntIterator iter = swapChanges.getRemoveFromP1().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPids().get(iter.next()).contains(pid1));
        }
        for(TIntIterator iter = swapChanges.getRemoveFromP2().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPids().get(iter.next()).contains(pid2));
        }

        assertTrue(state.getLogicalReplicaPartitions().get(pid1).containsAll(swapChanges.getAddToP1()));
        assertTrue(state.getLogicalReplicaPartitions().get(pid2).containsAll(swapChanges.getAddToP2()));

        for(TIntIterator iter = swapChanges.getRemoveFromP1().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPartitions().get(pid1).contains(iter.next()));
        }
        for(TIntIterator iter = swapChanges.getRemoveFromP2().iterator(); iter.hasNext(); ) {
            assertFalse(state.getLogicalReplicaPartitions().get(pid2).contains(iter.next()));
        }
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //For every friend of the moving user:
        //    We should add a replica in the target partition if there isn't already a replica or master in that partition
        assertEquals(findReplicasToAddToTargetPartition(1, 2, state), initSet(4, 12));
        assertEquals(findReplicasToAddToTargetPartition(1, 3, state), initSet(6, 8));
        assertEquals(findReplicasToAddToTargetPartition(13, 1, state), new TIntHashSet());
    }

    @Test
    public void testFindReplicasInMovingPartitionToDelete() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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
        friendships.put(11, initSet(9, 12));
        friendships.put(12, initSet(13));
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>();
        replicas.put(1, initSet( 6, 8, 9, 10, 12,  7, 13));
        replicas.put(2, initSet( 1, 2, 3,  5, 10, 11));
        replicas.put(3, initSet( 1, 2, 3,  4,  5,  9));

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //For every friend of the moving user, if:
        //    (1) friend has a replica on movingPartition
        //    (2) friend doesn't have any other friends on movingPartition
        //    (3) friend has more than minNumReplicas replicas
        //then delete it

        //All of its friends either have masters in P1, or have replicas in P1
        TIntSet toDeleteIn1For1 = findReplicasInMovingPartitionToDelete(1, 1, findReplicasToAddToTargetPartition(1, 2, state), state);
        assertEquals(toDeleteIn1For1, new TIntHashSet());

        //10 was only in P2 for 9's sake
        TIntSet toDeleteIn2For9 = findReplicasInMovingPartitionToDelete(9, 2, findReplicasToAddToTargetPartition(9, 3, state), state);
        assertEquals(toDeleteIn2For9, initSet(10));

        //10's friends are 1, 4, 9, and 11.  1 and 9 have other friends on P3.  11 is on P3.  4's only there for 10, but would violate minNumReplicas
        TIntSet toDeleteIn3For10 = findReplicasInMovingPartitionToDelete(10, 3, findReplicasToAddToTargetPartition(10, 1, state), state);
        assertEquals(toDeleteIn3For10, new TIntHashSet());
    }

    @Test
    public void testShouldDeleteReplicaInTargetPartitionAndShouldWeAddAReplicaOfMovingUserInMovingPartition() {
        int minNumReplicas = 1;
        float alpha = 1f;
        float initialT = 2f;
        float deltaT = 0.5f;
        int k = 7;

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

        SpajaRepartitioner.State state = new SpajaRepartitioner.State(minNumReplicas, alpha, initialT, deltaT, k, bidirectionalFriendships);
        fillState(state, partitions, replicas);

        //Test shouldDeleteReplicaInTargetPartition
        //If there's a replica of moving user in targetPartition, then delete it
        for(Integer pid : replicas.keys()) {
            for(Integer uid : friendships.keys()) {
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
        shouldWeAddAReplicaOfMovingUserInMovingPartition(1, 1, state);
        for(Integer pid : replicas.keys()) {
uidLoop:    for (Integer uid : friendships.keys()) {
                Integer logicalPid = state.getLogicalPids().get(uid);
                if(partitions.get(pid).contains(uid)) {
                    continue;
                }
                if(state.getLogicalReplicaPids().get(uid).contains(uid) && state.getLogicalReplicaPids().get(uid).size() <= state.getMinNumReplicas()) {
                    assertTrue(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
                    continue;
                }
                for(TIntIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
                    int friendId = iter.next();
                    if(state.getLogicalPids().get(friendId) == logicalPid) {
                        assertTrue(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
                        continue uidLoop;
                    }
                }
                assertFalse(shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, pid, state));
            }
        }
    }


    private static void fillState(SpajaRepartitioner.State state, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas) {
        state.setLogicalPids(getUToMasterMap(partitions));
        state.setLogicalReplicaPids(getUToReplicasMap(replicas, state.getFriendships().keySet()));
        state.setLogicalReplicaPartitions(replicas);
    }
}
