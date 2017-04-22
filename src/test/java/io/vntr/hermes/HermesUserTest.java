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

//    @Test
//    public void testGetLogicalUser() {
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
//        HermesManager manager = HermesTestUtils.initGraph(gamma, false, partitions, friendships);
//        for(Integer pid : manager.getPids()) {
//            manager.getPartition(pid).resetLogicalUsers();
//        }
//
//        Map<Integer, Integer> pToFriendCount = new HashMap<>();
//        pToFriendCount.put(1, 4);
//        pToFriendCount.put(2, 2);
//        pToFriendCount.put(3, 0);
//
//        Map<Integer, Integer> pToWeight = new HashMap<>();
//        pToWeight.put(1, 7);
//        pToWeight.put(2, 7);
//        pToWeight.put(3, 6);
//        LogicalUser expectedLuser = new LogicalUser(12, 2, gamma, pToFriendCount, pToWeight, 20);
//        assertEquals(manager.getUser(12).getLogicalUser(true), expectedLuser);
//        assertEquals(manager.getUser(12).getLogicalUser(false), expectedLuser);
//
//        manager.migrateLogically(new Target(3, 3, 1, 0));
//
//        pToFriendCount.put(1, 3);
//        pToFriendCount.put(2, 2);
//        pToFriendCount.put(3, 1);
//        Map<Integer, Integer> logicalPToWeight = new HashMap<>();
//        logicalPToWeight.put(1, 6);
//        logicalPToWeight.put(2, 7);
//        logicalPToWeight.put(3, 7);
//
//        LogicalUser expectedLuserPhysicalWeights = new LogicalUser(12, 2, gamma, pToFriendCount, pToWeight, 20);
//        LogicalUser expectedLuserLogicalWeights = new LogicalUser(12, 2, gamma, pToFriendCount, logicalPToWeight, 20);
//        assertEquals(manager.getUser(12).getLogicalUser(true), expectedLuserPhysicalWeights);
//        assertEquals(manager.getUser(12).getLogicalUser(false), expectedLuserLogicalWeights);
//
//        manager.migrateLogically(new Target(12, 3, 2, 0));
//
//        logicalPToWeight.put(1, 6);
//        logicalPToWeight.put(2, 6);
//        logicalPToWeight.put(3, 8);
//
//        expectedLuserPhysicalWeights = new LogicalUser(12, 3, gamma, pToFriendCount, pToWeight, 20);
//        expectedLuserLogicalWeights = new LogicalUser(12, 3, gamma, pToFriendCount, logicalPToWeight, 20);
//
//        assertEquals(manager.getUser(12).getLogicalUser(true), expectedLuserPhysicalWeights);
//        assertEquals(manager.getUser(12).getLogicalUser(false), expectedLuserLogicalWeights);
//    }

}
