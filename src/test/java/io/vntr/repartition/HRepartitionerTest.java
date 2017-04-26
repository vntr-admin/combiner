package io.vntr.repartition;

import io.vntr.utils.ProbabilityUtils;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.Utils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class HRepartitionerTest {

    @Test
    public void testInitState() {
        float gamma = 1.05f;

        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
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
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        assertEquals(state.getLogicalPartitions(), partitions);

        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        assertEquals(friendships.keySet(), state.getLogicalUsers().keySet());
        for(int uid : friendships.keySet()) {
            HRepartitioner.LogicalUser user = state.getLogicalUsers().get(uid);

            assertEquals(user.getId(), (Integer) uid);
            assertEquals(user.getPid(), uidToPidMap.get(uid));
            assertEquals(user.getTotalWeight(), (Integer) friendships.size());

            Map<Integer, Integer> pToWeight = user.getpToWeight();
            for(int pid : partitions.keySet()) {
                assertEquals(pToWeight.get(pid), (Integer) partitions.get(pid).size());
            }

            Map<Integer, Integer> pToFriendCount = user.getpToFriendCount();
            for(int pid : partitions.keySet()) {
                Set<Integer> friends = new HashSet<>(bidirectionalFriendships.get(uid));
                friends.retainAll(partitions.get(pid));
                assertTrue(pToFriendCount.get(pid) == friends.size());
            }
        }
    }

    @Test
    public void testLogicallyMigrateAndUpdateLogicalUsers() {
        float gamma = 1.05f;

        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
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
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        Integer uid1 = 1;
        Integer pid1 = 1;

        Integer uid2 = 8;
        Integer pid2 = 2;

        assertTrue(state.getLogicalPartitions().get(pid1).contains(uid1));
        assertTrue(state.getLogicalPartitions().get(pid2).contains(uid2));

        assertEquals(state.getLogicalUsers().get(uid1).getPid(), pid1);
        assertEquals(state.getLogicalUsers().get(uid2).getPid(), pid2);

        Set<Target> targets = initSet(new Target(uid1, pid2, pid1, 1f), new Target(uid2, pid1, pid2, 1f));

        HRepartitioner.logicallyMigrate(targets, state);
        state.updateLogicalUsers(friendships, gamma);

        assertTrue(state.getLogicalPartitions().get(pid1).contains(uid2));
        assertTrue(state.getLogicalPartitions().get(pid2).contains(uid1));

        assertEquals(state.getLogicalUsers().get(uid1).getPid(), pid2);
        assertEquals(state.getLogicalUsers().get(uid2).getPid(), pid1);
    }

    @Test
    public void testGetCandidatesFirstStage() {
        float gamma = 1.5f;

        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put(2,  initSet(3, 6, 9, 12));
        friendships.put(3,  initSet(4, 8, 12));
        friendships.put(4,  initSet(5, 10));
        friendships.put(5,  initSet(6, 12));
        friendships.put(6,  initSet(7, 12, 13));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        Set<Target> targets = HRepartitioner.getCandidates(2, true, 3, state);
        assertEquals(targets, initSet(new Target(6, 3, 2, 1f)));
    }

    @Test
    public void testGetCandidatesSecondStage() {
        float gamma = 1.5f;

        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put(2,  initSet(3, 6, 9, 12));
        friendships.put(3,  initSet(4, 8, 12));
        friendships.put(4,  initSet(5, 10));
        friendships.put(5,  initSet(6, 12));
        friendships.put(6,  initSet(7, 12, 13));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        HRepartitioner.State state = HRepartitioner.initState(partitions, bidirectionalFriendships, gamma);

        Set<Target> targets = HRepartitioner.getCandidates(2, false, 3, state);
        assertEquals(targets, initSet(new Target(6, 1, 2, 2f)));
    }
}
