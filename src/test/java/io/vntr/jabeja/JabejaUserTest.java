package io.vntr.jabeja;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static io.vntr.jabeja.JabejaTestUtils.*;
import static io.vntr.TestUtils.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaUserTest {
    private Map<Integer, Set<Integer>> fig2aPartitions;
    private Map<Integer, Set<Integer>> fig2aFriendships;
    private Map<Integer, Set<Integer>> fig2bPartitions;
    private Map<Integer, Set<Integer>> fig2bFriendships;

    @Before
    public void init() {
        fig2aPartitions  = new HashMap<Integer, Set<Integer>>();
        fig2aFriendships = new HashMap<Integer, Set<Integer>>();
        fig2bPartitions  = new HashMap<Integer, Set<Integer>>();
        fig2bFriendships = new HashMap<Integer, Set<Integer>>();

        fig2aPartitions.put(1, initSet(1, 2, 3, 4, 5));
        fig2aPartitions.put(2, initSet(6, 7));
        fig2aFriendships.put(1, initSet(2, 6));
        fig2aFriendships.put(2, initSet(6));
        fig2aFriendships.put(3, initSet(7));
        fig2aFriendships.put(4, initSet(7));
        fig2aFriendships.put(5, initSet(7));
        fig2aFriendships.put(6, Collections.<Integer>emptySet());
        fig2aFriendships.put(7, Collections.<Integer>emptySet());

        fig2bPartitions.put(1, initSet(1, 2, 3, 4, 5, 6));
        fig2bPartitions.put(2, initSet(7, 8, 9, 10));
        fig2bFriendships.put(1, initSet(3, 7));
        fig2bFriendships.put(2, initSet(3));
        fig2bFriendships.put(3, initSet(7));
        fig2bFriendships.put(4, initSet(6, 8, 10));
        fig2bFriendships.put(5, initSet(6, 10));
        fig2bFriendships.put(6, initSet(9, 10));
        fig2bFriendships.put(8, initSet(10));
        fig2bFriendships.put(9, initSet(10));
        fig2bFriendships.put(10, Collections.<Integer>emptySet());
    }

    @Test
    public void testGetNeighborsOnPartition()
    {
        float alpha = 1.1f;
        JabejaManager manager = new JabejaManager(alpha, 2f, .2f, 9);
        Integer pid1 = manager.addPartition();
        Integer pid2 = manager.addPartition();

        JabejaUser user1 = new JabejaUser(1, pid1, alpha, manager);
        JabejaUser user2 = new JabejaUser(2, pid1, alpha, manager);
        JabejaUser user3 = new JabejaUser(3, pid1, alpha, manager);
        JabejaUser user4 = new JabejaUser(4, pid2, alpha, manager);
        JabejaUser user5 = new JabejaUser(5, pid2, alpha, manager);
        JabejaUser user6 = new JabejaUser(6, pid2, alpha, manager);

        manager.addUser(user1);
        manager.addUser(user2);
        manager.addUser(user3);
        manager.addUser(user4);
        manager.addUser(user5);
        manager.addUser(user6);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 0);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 0);

        manager.befriend(1, 2);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 0);

        manager.befriend(1, 3);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 0);

        manager.befriend(1, 4);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 1);

        manager.befriend(1, 5);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.befriend(1, 6);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.befriend(2, 3);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.befriend(4, 5);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.unfriend(1, 2);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.unfriend(1, 4);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.unfriend(4, 5);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.befriend(1, 2);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.removeUser(3);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.removeUser(6);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 1);

        manager.addUser(user3);
        manager.addUser(user6);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 1);

        manager.befriend(1, 3);
        manager.befriend(1, 4);
        manager.befriend(1, 6);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        Integer pid3 = manager.addPartition();

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);
        assertTrue(user1.getNeighborsOnPartition(pid3) == 0);

        JabejaMiddleware middleware = new JabejaMiddleware(manager);

        middleware.removePartition(pid1);

        assertTrue(user1.getNeighborsOnPartition(pid2) + user1.getNeighborsOnPartition(pid3) == 5);

        middleware.removePartition(pid2);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 5);

        Integer pid4 = manager.addPartition();
        manager.moveUser(2, pid4);
        manager.moveUser(4, pid4);
        manager.moveUser(6, pid4);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid4) == 3);

        manager.unfriend(1, 2);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid4) == 2);

        manager.swap(2, 3);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid4) == 3);
    }

    @Test
    public void testFindPartnerPrimary() {
        float alpha = 1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<Integer, Set<Integer>>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<Integer, Set<Integer>>();
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

        JabejaManager manager = initGraph(alpha, 2f, .2f, 9, partitions, friendships);

        //Scenarios:
        //    2: ensure that negative gain is impossible with T=1, and possible with T > 1
        //    3: make sure alpha does whatever it's supposed to do


        //Scenario 1: ensure that the best partner is chosen
        Collection<JabejaUser> col = getUsers(manager, 1, 2, 3, 4, 5, 6, 7, 8);
        assertEquals(manager.getUser(1), manager.getUser(12).findPartner(col, 1f));

        col = getUsers(manager, 2, 3, 4, 5, 6, 7, 8);
        assertEquals(manager.getUser(2), manager.getUser(12).findPartner(col, 1f));

        col = getUsers(manager, 6, 7, 8, 9, 10, 11, 12, 13);
        assertEquals(manager.getUser(12), manager.getUser(1).findPartner(col, 1f));

        col = getUsers(manager, 6, 7, 8);
        assertNull(manager.getUser(11).findPartner(col, 1f));
        assertNull(manager.getUser(11).findPartner(col, 100f));
    }

    @Test
    public void testFindPartnerEffectOfT() {
        float alpha = 1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<Integer, Set<Integer>>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<Integer, Set<Integer>>();
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

        JabejaManager manager = initGraph(alpha, 2f, .2f, 9, partitions, friendships);

        //Scenario 2: ensure that non-positive gain is impossible with T=1, and possible with T > 1
        Collection<JabejaUser> col = getUsers(manager, 6, 7, 8, 9);
        assertNull(manager.getUser(10).findPartner(col, 1f));
        assertEquals(manager.getUser(9), manager.getUser(10).findPartner(col, 1.001f));
    }

    @Test
    public void testFindPartnerEffectOfAlpha() {
        //Taken directly from figure 2 of Ja-be-Ja: A Distributed Algorithm for Balanced Graph Partitioning
        //SICS Technical Report T2013:03
        //ISSN 1100-3154

        JabejaManager fig2aAlpha1     = initGraph(1f,     2f, .2f, 9, fig2aPartitions, fig2aFriendships);
        JabejaManager fig2aAlpha1_001 = initGraph(1.001f, 2f, .2f, 9, fig2aPartitions, fig2aFriendships);

        JabejaManager fig2bAlpha1     = initGraph(1f,     2f, .2f, 9, fig2bPartitions, fig2bFriendships);
        JabejaManager fig2bAlpha1_001 = initGraph(1.001f, 2f, .2f, 9, fig2bPartitions, fig2bFriendships);

        assertEquals(fig2aAlpha1.getUser    (7), fig2aAlpha1.getUser    (2).findPartner(getUsers(fig2aAlpha1,     7), 1f));
        assertEquals(fig2aAlpha1.getUser    (2), fig2aAlpha1.getUser    (7).findPartner(getUsers(fig2aAlpha1,     2), 1f));
        assertEquals(fig2aAlpha1_001.getUser(7), fig2aAlpha1_001.getUser(2).findPartner(getUsers(fig2aAlpha1_001, 7), 1f));
        assertEquals(fig2aAlpha1_001.getUser(2), fig2aAlpha1_001.getUser(7).findPartner(getUsers(fig2aAlpha1_001, 2), 1f));

        assertNull(fig2bAlpha1.getUser    (3).findPartner(getUsers(fig2bAlpha1,     10), 1f));
        assertNull(fig2bAlpha1.getUser    (10).findPartner(getUsers(fig2bAlpha1,     3), 1f));
        assertEquals(fig2bAlpha1_001.getUser(10), fig2bAlpha1_001.getUser(3).findPartner(getUsers(fig2bAlpha1_001, 10), 1f));
        assertEquals(fig2bAlpha1_001.getUser(3), fig2bAlpha1_001.getUser(10).findPartner(getUsers(fig2bAlpha1_001, 3), 1f));
    }
}
