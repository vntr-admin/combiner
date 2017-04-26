package io.vntr.hermes;

import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class HermesPartitionTest {

//    @Test
//    public void testGetCandidatesNoUnderweightNoOverweight() {
//        float gamma = 1.5f;
//        Map<Integer, Set<Integer>> partitions = new HashMap<>();
//        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
//        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
//        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));
//
//        Map<Integer, Set<Integer>> friendships = new HashMap<>();
//        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
//        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
//        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
//        friendships.put( 4, initSet( 5, 10, 15));
//        friendships.put( 5, initSet( 6, 12, 18));
//        friendships.put( 6, initSet( 7, 14));
//        friendships.put( 7, initSet( 8, 16));
//        friendships.put( 8, initSet( 9, 18));
//        friendships.put( 9, initSet(10, 20));
//        friendships.put(10, initSet(11));
//        friendships.put(11, initSet(12));
//        friendships.put(12, initSet(13));
//        friendships.put(13, initSet(14));
//        friendships.put(14, initSet(15));
//        friendships.put(15, initSet(16));
//        friendships.put(16, initSet(17));
//        friendships.put(17, initSet(18));
//        friendships.put(18, initSet(19));
//        friendships.put(19, initSet(20));
//        friendships.put(20, Collections.<Integer>emptySet());
//
//        HManager manager = HermesTestUtils.initGraph(gamma, false, partitions, friendships);
//        for(Integer pid : manager.getPids()) {
//            manager.getPartition(pid).resetLogicalUsers();
//        }
//
//        Set<Target> oneUp   = initSet(new Target(1, 2, 1, 1));
//        Set<Target> oneDown = Collections.emptySet(); //1 is the smallest PID, so there's no down
//
//        Set<Target> twoUp   = Collections.emptySet(); //the gains are all non-positive
//        Set<Target> twoDown = initSet(new Target(8, 1, 2, 2), new Target(12, 1, 2, 2), new Target(14, 1, 2, 1));
//
//        Set<Target> threeUp = Collections.emptySet(); //3 is the largest PID, so there's no up
//        Set<Target> threeDown = initSet(new Target(16, 1, 3, 1), new Target(18, 1, 3, 1), new Target(20, 1, 3, 1));
//
//        assertEquals(oneUp,     manager.getPartition(1).getCandidates(true,  3, false));
//        assertEquals(oneDown,   manager.getPartition(1).getCandidates(false, 3, false));
//        assertEquals(twoUp,     manager.getPartition(2).getCandidates(true,  3, false));
//        assertEquals(twoDown,   manager.getPartition(2).getCandidates(false, 3, false));
//        assertEquals(threeUp,   manager.getPartition(3).getCandidates(true,  3, false));
//        assertEquals(threeDown, manager.getPartition(3).getCandidates(false, 3, false));
//    }
//
//    @Test
//    public void testGetCandidatesUnderweightOverweight() {
//        float gamma = 1.3f; //underweight is < 2.8, overweight is > 5.2
//        Map<Integer, Set<Integer>> partitions = new HashMap<>();
//        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
//        partitions.put(2, initSet( 8,  9, 10, 11));
//        partitions.put(3, initSet(11, 12));
//
//        Map<Integer, Set<Integer>> friendships = new HashMap<>();
//        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12));
//        friendships.put( 2, initSet( 3,  6,  9, 12));
//        friendships.put( 3, initSet( 4,  8, 12));
//        friendships.put( 4, initSet( 5, 10));
//        friendships.put( 5, initSet( 6, 12));
//        friendships.put( 6, initSet( 7));
//        friendships.put( 7, initSet( 8));
//        friendships.put( 8, initSet( 9));
//        friendships.put( 9, initSet(10));
//        friendships.put(10, initSet(11));
//        friendships.put(11, initSet(12));
//        friendships.put(12, Collections.<Integer>emptySet());
//
//        HManager manager = HermesTestUtils.initGraph(gamma, false, partitions, friendships);
//        for(Integer pid : manager.getPids()) {
//            manager.getPartition(pid).resetLogicalUsers();
//        }
//
//        assertEquals(Collections.emptySet(), manager.getPartition(1).getCandidates(false, 3, false));
//        assertEquals(Collections.emptySet(), manager.getPartition(2).getCandidates(true,  3, false));
//        assertEquals(Collections.emptySet(), manager.getPartition(2).getCandidates(false, 3, false));
//        assertEquals(Collections.emptySet(), manager.getPartition(3).getCandidates(true,  3, false));
//        assertEquals(Collections.emptySet(), manager.getPartition(3).getCandidates(false, 3, false));
//
//        Target bestTarget = new Target(7, 2, 1, 0);
//        Set<Target> secondPlaceTargets = initSet(new Target(1, 2, 1, -1), new Target(3, 2, 1, -1), new Target(3, 3, 1, -1), new Target(5, 3, 1, -1));
//
//        Set<Target> results = manager.getPartition(1).getCandidates(true,  3, false);
//        assertTrue(results.size() == 3);
//        assertTrue(results.contains(bestTarget));
//        secondPlaceTargets.retainAll(results);
//        assertTrue(secondPlaceTargets.size() == 2);
//    }
//
//    @Test
//    public void testPhysicallyMigrateCopyAndDelete() {
//        float gamma = 1.5f;
//        Map<Integer, Set<Integer>> partitions = new HashMap<>();
//        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
//        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
//        partitions.put(3, initSet(15, 16, 17, 18,  19, 20));
//
//        Map<Integer, Set<Integer>> friendships = new HashMap<>();
//        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
//        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
//        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
//        friendships.put( 4, initSet( 5, 10, 15));
//        friendships.put( 5, initSet( 6, 12, 18));
//        friendships.put( 6, initSet( 7, 14));
//        friendships.put( 7, initSet( 8, 16));
//        friendships.put( 8, initSet( 9, 18));
//        friendships.put( 9, initSet(10, 20));
//        friendships.put(10, initSet(11));
//        friendships.put(11, initSet(12));
//        friendships.put(12, initSet(13));
//        friendships.put(13, initSet(14));
//        friendships.put(14, initSet(15));
//        friendships.put(15, initSet(16));
//        friendships.put(16, initSet(17));
//        friendships.put(17, initSet(18));
//        friendships.put(18, initSet(19));
//        friendships.put(19, initSet(20));
//        friendships.put(20, Collections.<Integer>emptySet());
//
//        HManager manager = HermesTestUtils.initGraph(gamma, false, partitions, friendships);
//        for(Integer pid : manager.getPids()) {
//            manager.getPartition(pid).resetLogicalUsers();
//        }
//
//        assertEquals(partitions, manager.getPartitionToLogicalUserMap());
//        assertEquals(partitions, manager.getPartitionToUsers());
//
//        manager.migrateLogically(new Target( 3, 3, 1, 0));
//        manager.migrateLogically(new Target( 4, 3, 1, 0));
//        manager.migrateLogically(new Target(20, 1, 3, 0));
//
//        assertEquals(initSet(3, 4), manager.getPartition(3).physicallyMigrateCopy());
//
//        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7));
//        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
//        partitions.put(3, initSet( 3,  4, 15, 16, 17, 18, 19, 20));
//
//        assertEquals(partitions, manager.getPartitionToUsers());
//
//        partitions.put(1, initSet( 1,  2,  5,  6,  7, 20));
//        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
//        partitions.put(3, initSet( 3,  4, 15, 16, 17, 18, 19));
//
//        assertEquals(partitions, manager.getPartitionToLogicalUserMap());
//
//        assertEquals(Collections.emptySet(), manager.getPartition(2).physicallyMigrateCopy());
//        assertEquals(initSet(20), manager.getPartition(1).physicallyMigrateCopy());
//
//        partitions.put(1, initSet( 1,  2,  3,  4,  5,  6,  7, 20));
//        partitions.put(2, initSet( 8,  9, 10, 11, 12, 13, 14));
//        partitions.put(3, initSet( 3,  4, 15, 16, 17, 18, 19, 20));
//
//        assertEquals(partitions, manager.getPartitionToUsers());
//
//        manager.getPartition(1).physicallyMigrateDelete();
//
//        partitions.put(1, initSet( 1,  2,  5,  6,  7, 20));
//
//        assertEquals(partitions, manager.getPartitionToUsers());
//
//        manager.getPartition(2).physicallyMigrateDelete();
//
//        assertEquals(partitions, manager.getPartitionToUsers());
//
//        manager.getPartition(3).physicallyMigrateDelete();
//
//        partitions.put(3, initSet( 3,  4, 15, 16, 17, 18, 19));
//
//        assertEquals(partitions, manager.getPartitionToUsers());
//    }
}
