package io.vntr.hermes;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.assertEquals;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class HermesPartitionTest {

    @Test
    public void testGetCandidatesNoUnderweightNoOverweight() {
        double gamma = 1.5;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L,  6L,  7L));
        partitions.put(2L, initSet( 8L,  9L, 10L, 11L, 12L, 13L, 14L));
        partitions.put(3L, initSet(15L, 16L, 17L, 18L,  19L, 20L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L, 15L, 18L));
        friendships.put( 3L, initSet( 4L,  8L, 12L, 16L, 20L));
        friendships.put( 4L, initSet( 5L, 10L, 15L));
        friendships.put( 5L, initSet( 6L, 12L, 18L));
        friendships.put( 6L, initSet( 7L, 14L));
        friendships.put( 7L, initSet( 8L, 16L));
        friendships.put( 8L, initSet( 9L, 18L));
        friendships.put( 9L, initSet(10L, 20L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, initSet(14L));
        friendships.put(14L, initSet(15L));
        friendships.put(15L, initSet(16L));
        friendships.put(16L, initSet(17L));
        friendships.put(17L, initSet(18L));
        friendships.put(18L, initSet(19L));
        friendships.put(19L, initSet(20L));
        friendships.put(20L, Collections.<Long>emptySet());

        HermesManager manager = HermesTestUtils.initGraph(gamma, partitions, friendships);
        for(Long pid : manager.getAllPartitionIds()) {
            manager.getPartitionById(pid).resetLogicalUsers();
        }

        Set<Target> twoDown = initSet(new Target(8L, 1L, 2L, 2), new Target(12L, 1L, 2L, 2), new Target(14L, 1L, 2L, 1));
        Set<Target> threeDown = initSet(new Target(16L, 1L, 3L, 1), new Target(18L, 1L, 3L, 1), new Target(20L, 1L, 3L, 1));

        assertEquals(initSet(new Target(1L, 2L, 1L, 1)), manager.getPartitionById(1L).getCandidates(true,  3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(1L).getCandidates(false, 3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(2L).getCandidates(true,  3));
        assertEquals(twoDown, manager.getPartitionById(2L).getCandidates(false, 3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(3L).getCandidates(true,  3));
        assertEquals(threeDown, manager.getPartitionById(3L).getCandidates(false, 3));
    }

    @Test
    public void testGetCandidatesUnderweightOverweight() {
        double gamma = 1.3; //underweight is < 2.8, overweight is > 5.2
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L,  6L,  7L));
        partitions.put(2L, initSet( 8L,  9L, 10L, 11L));
        partitions.put(3L, initSet(11L, 12L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L));
        friendships.put( 3L, initSet( 4L,  8L, 12L));
        friendships.put( 4L, initSet( 5L, 10L));
        friendships.put( 5L, initSet( 6L, 12L));
        friendships.put( 6L, initSet( 7L));
        friendships.put( 7L, initSet( 8L));
        friendships.put( 8L, initSet( 9L));
        friendships.put( 9L, initSet(10L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, Collections.<Long>emptySet());

        HermesManager manager = HermesTestUtils.initGraph(gamma, partitions, friendships);
        for(Long pid : manager.getAllPartitionIds()) {
            manager.getPartitionById(pid).resetLogicalUsers();
        }

        Set<Target> oneUp = initSet(new Target(3L, 2L, 1L, -1), new Target(5L, 3L, 1L, -1), new Target(7L, 2L, 1L, 0));

        assertEquals(oneUp, manager.getPartitionById(1L).getCandidates(true,  3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(1L).getCandidates(false, 3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(2L).getCandidates(true,  3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(2L).getCandidates(false, 3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(3L).getCandidates(true,  3));
        assertEquals(Collections.emptySet(), manager.getPartitionById(3L).getCandidates(false, 3));
    }

    @Test
    public void testPhysicallyMigrateCopyAndDelete() {
        double gamma = 1.5;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L,  6L,  7L));
        partitions.put(2L, initSet( 8L,  9L, 10L, 11L, 12L, 13L, 14L));
        partitions.put(3L, initSet(15L, 16L, 17L, 18L,  19L, 20L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put( 1L, initSet( 2L,  4L,  6L,  8L, 10L, 12L, 14L, 16L, 18L, 20L));
        friendships.put( 2L, initSet( 3L,  6L,  9L, 12L, 15L, 18L));
        friendships.put( 3L, initSet( 4L,  8L, 12L, 16L, 20L));
        friendships.put( 4L, initSet( 5L, 10L, 15L));
        friendships.put( 5L, initSet( 6L, 12L, 18L));
        friendships.put( 6L, initSet( 7L, 14L));
        friendships.put( 7L, initSet( 8L, 16L));
        friendships.put( 8L, initSet( 9L, 18L));
        friendships.put( 9L, initSet(10L, 20L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, initSet(14L));
        friendships.put(14L, initSet(15L));
        friendships.put(15L, initSet(16L));
        friendships.put(16L, initSet(17L));
        friendships.put(17L, initSet(18L));
        friendships.put(18L, initSet(19L));
        friendships.put(19L, initSet(20L));
        friendships.put(20L, Collections.<Long>emptySet());

        HermesManager manager = HermesTestUtils.initGraph(gamma, partitions, friendships);
        for(Long pid : manager.getAllPartitionIds()) {
            manager.getPartitionById(pid).resetLogicalUsers();
        }

        assertEquals(partitions, manager.getPartitionToLogicalUserMap());
        assertEquals(partitions, manager.getPartitionToUserMap());

        manager.migrateLogically(new Target( 3L, 3L, 1L, 0));
        manager.migrateLogically(new Target( 4L, 3L, 1L, 0));
        manager.migrateLogically(new Target(20L, 1L, 3L, 0));

        assertEquals(initSet(3L, 4L), manager.getPartitionById(3L).physicallyMigrateCopy());

        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L,  6L,  7L));
        partitions.put(2L, initSet( 8L,  9L, 10L, 11L, 12L, 13L, 14L));
        partitions.put(3L, initSet( 3L,  4L, 15L, 16L, 17L, 18L, 19L, 20L));

        assertEquals(partitions, manager.getPartitionToUserMap());

        partitions.put(1L, initSet( 1L,  2L,  5L,  6L,  7L, 20L));
        partitions.put(2L, initSet( 8L,  9L, 10L, 11L, 12L, 13L, 14L));
        partitions.put(3L, initSet( 3L,  4L, 15L, 16L, 17L, 18L, 19L));

        assertEquals(partitions, manager.getPartitionToLogicalUserMap());

        assertEquals(Collections.emptySet(), manager.getPartitionById(2L).physicallyMigrateCopy());
        assertEquals(initSet(20L), manager.getPartitionById(1L).physicallyMigrateCopy());

        partitions.put(1L, initSet( 1L,  2L,  3L,  4L,  5L,  6L,  7L, 20L));
        partitions.put(2L, initSet( 8L,  9L, 10L, 11L, 12L, 13L, 14L));
        partitions.put(3L, initSet( 3L,  4L, 15L, 16L, 17L, 18L, 19L, 20L));

        assertEquals(partitions, manager.getPartitionToUserMap());

        manager.getPartitionById(1L).physicallyMigrateDelete();

        partitions.put(1L, initSet( 1L,  2L,  5L,  6L,  7L, 20L));

        assertEquals(partitions, manager.getPartitionToUserMap());

        manager.getPartitionById(2L).physicallyMigrateDelete();

        assertEquals(partitions, manager.getPartitionToUserMap());

        manager.getPartitionById(3L).physicallyMigrateDelete();

        partitions.put(3L, initSet( 3L,  4L, 15L, 16L, 17L, 18L, 19L));

        assertEquals(partitions, manager.getPartitionToUserMap());
    }
}
