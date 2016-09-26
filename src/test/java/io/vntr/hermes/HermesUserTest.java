package io.vntr.hermes;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static io.vntr.TestUtils.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesUserTest {

    @Test
    public void testGetLogicalUser() {
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

        Map<Long, Long> pToFriendCount = new HashMap<Long, Long>();
        pToFriendCount.put(1L, 4L);
        pToFriendCount.put(2L, 2L);
        pToFriendCount.put(3L, 0L);

        Map<Long, Long> pToWeight = new HashMap<Long, Long>();
        pToWeight.put(1L, 7L);
        pToWeight.put(2L, 7L);
        pToWeight.put(3L, 6L);
        LogicalUser expectedLuser = new LogicalUser(12L, 2L, gamma, pToFriendCount, pToWeight, 20L);
        assertEquals(manager.getUser(12L).getLogicalUser(true), expectedLuser);
        assertEquals(manager.getUser(12L).getLogicalUser(false), expectedLuser);

        manager.migrateLogically(new Target(3L, 3L, 1L, 0));

        pToFriendCount.put(1L, 3L);
        pToFriendCount.put(2L, 2L);
        pToFriendCount.put(3L, 1L);
        Map<Long, Long> logicalPToWeight = new HashMap<Long, Long>();
        logicalPToWeight.put(1L, 6L);
        logicalPToWeight.put(2L, 7L);
        logicalPToWeight.put(3L, 7L);

        LogicalUser expectedLuserPhysicalWeights = new LogicalUser(12L, 2L, gamma, pToFriendCount, pToWeight, 20L);
        LogicalUser expectedLuserLogicalWeights = new LogicalUser(12L, 2L, gamma, pToFriendCount, logicalPToWeight, 20L);
        assertEquals(manager.getUser(12L).getLogicalUser(true), expectedLuserPhysicalWeights);
        assertEquals(manager.getUser(12L).getLogicalUser(false), expectedLuserLogicalWeights);

        manager.migrateLogically(new Target(12L, 3L, 2L, 0));

        logicalPToWeight.put(1L, 6L);
        logicalPToWeight.put(2L, 6L);
        logicalPToWeight.put(3L, 8L);

        expectedLuserPhysicalWeights = new LogicalUser(12L, 3L, gamma, pToFriendCount, pToWeight, 20L);
        expectedLuserLogicalWeights = new LogicalUser(12L, 3L, gamma, pToFriendCount, logicalPToWeight, 20L);

        assertEquals(manager.getUser(12L).getLogicalUser(true), expectedLuserPhysicalWeights);
        assertEquals(manager.getUser(12L).getLogicalUser(false), expectedLuserLogicalWeights);
    }

}
