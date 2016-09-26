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
    private Map<Long, Set<Long>> fig2aPartitions;
    private Map<Long, Set<Long>> fig2aFriendships;
    private Map<Long, Set<Long>> fig2bPartitions;
    private Map<Long, Set<Long>> fig2bFriendships;

    @Before
    public void init() {
        fig2aPartitions  = new HashMap<Long, Set<Long>>();
        fig2aFriendships = new HashMap<Long, Set<Long>>();
        fig2bPartitions  = new HashMap<Long, Set<Long>>();
        fig2bFriendships = new HashMap<Long, Set<Long>>();

        fig2aPartitions.put(1L, initSet(1L, 2L, 3L, 4L, 5L));
        fig2aPartitions.put(2L, initSet(6L, 7L));
        fig2aFriendships.put(1L, initSet(2L, 6L));
        fig2aFriendships.put(2L, initSet(6L));
        fig2aFriendships.put(3L, initSet(7L));
        fig2aFriendships.put(4L, initSet(7L));
        fig2aFriendships.put(5L, initSet(7L));
        fig2aFriendships.put(6L, Collections.<Long>emptySet());
        fig2aFriendships.put(7L, Collections.<Long>emptySet());

        fig2bPartitions.put(1L, initSet(1L, 2L, 3L, 4L, 5L, 6L));
        fig2bPartitions.put(2L, initSet(7L, 8L, 9L, 10L));
        fig2bFriendships.put(1L, initSet(3L, 7L));
        fig2bFriendships.put(2L, initSet(3L));
        fig2bFriendships.put(3L, initSet(7L));
        fig2bFriendships.put(4L, initSet(6L, 8L, 10L));
        fig2bFriendships.put(5L, initSet(6L, 10L));
        fig2bFriendships.put(6L, initSet(9L, 10L));
        fig2bFriendships.put(8L, initSet(10L));
        fig2bFriendships.put(9L, initSet(10L));
        fig2bFriendships.put(10L, Collections.<Long>emptySet());
    }

    @Test
    public void testGetNeighborsOnPartition()
    {
        double alpha = 1.1D;
        JabejaManager manager = new JabejaManager(alpha, 2D, .2D, 9);
        Long pid1 = manager.addPartition();
        Long pid2 = manager.addPartition();

        JabejaUser user1 = new JabejaUser("User 1", 1L, pid1, alpha, manager);
        JabejaUser user2 = new JabejaUser("User 2", 2L, pid1, alpha, manager);
        JabejaUser user3 = new JabejaUser("User 3", 3L, pid1, alpha, manager);
        JabejaUser user4 = new JabejaUser("User 4", 4L, pid2, alpha, manager);
        JabejaUser user5 = new JabejaUser("User 5", 5L, pid2, alpha, manager);
        JabejaUser user6 = new JabejaUser("User 6", 6L, pid2, alpha, manager);

        manager.addUser(user1);
        manager.addUser(user2);
        manager.addUser(user3);
        manager.addUser(user4);
        manager.addUser(user5);
        manager.addUser(user6);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 0);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 0);

        manager.befriend(1L, 2L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 0);

        manager.befriend(1L, 3L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 0);

        manager.befriend(1L, 4L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 1);

        manager.befriend(1L, 5L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.befriend(1L, 6L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.befriend(2L, 3L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.befriend(4L, 5L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.unfriend(1L, 2L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        manager.unfriend(1L, 4L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.unfriend(4L, 5L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.befriend(1L, 2L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.removeUser(3L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 2);

        manager.removeUser(6L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 1);

        manager.addUser(user3);
        manager.addUser(user6);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 1);

        manager.befriend(1L, 3L);
        manager.befriend(1L, 4L);
        manager.befriend(1L, 6L);

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);

        Long pid3 = manager.addPartition();

        assertTrue(user1.getNeighborsOnPartition(pid1) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid2) == 3);
        assertTrue(user1.getNeighborsOnPartition(pid3) == 0);

        JabejaMiddleware middleware = new JabejaMiddleware(manager);

        middleware.removePartition(pid1);

        assertTrue(user1.getNeighborsOnPartition(pid2) + user1.getNeighborsOnPartition(pid3) == 5);

        middleware.removePartition(pid2);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 5);

        Long pid4 = manager.addPartition();
        manager.moveUser(2L, pid4);
        manager.moveUser(4L, pid4);
        manager.moveUser(6L, pid4);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid4) == 3);

        manager.unfriend(1L, 2L);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 2);
        assertTrue(user1.getNeighborsOnPartition(pid4) == 2);

        manager.swap(2L, 3L);

        assertTrue(user1.getNeighborsOnPartition(pid3) == 1);
        assertTrue(user1.getNeighborsOnPartition(pid4) == 3);
    }

    @Test
    public void testFindPartnerPrimary() {
        double alpha = 1D;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L, 5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L));
        partitions.put(3L, initSet(10L, 11L, 12L, 13L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put(1L,  initSet(2L, 4L, 6L, 8L, 10L, 12L));
        friendships.put(2L,  initSet(3L, 6L, 9L, 12L));
        friendships.put(3L,  initSet(4L, 8L, 12L));
        friendships.put(4L,  initSet(5L, 10L));
        friendships.put(5L,  initSet(6L, 12L));
        friendships.put(6L,  initSet(7L));
        friendships.put(7L,  initSet(8L));
        friendships.put(8L,  initSet(9L));
        friendships.put(9L,  initSet(10L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, Collections.<Long>emptySet());

        JabejaManager manager = initGraph(alpha, 2D, .2D, 9, partitions, friendships);

        //Scenarios:
        //    2: ensure that negative gain is impossible with T=1, and possible with T > 1
        //    3: make sure alpha does whatever it's supposed to do


        //Scenario 1: ensure that the best partner is chosen
        Collection<JabejaUser> col = getUsers(manager, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertEquals(manager.getUser(1L), manager.getUser(12L).findPartner(col, 1D));

        col = getUsers(manager, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertEquals(manager.getUser(2L), manager.getUser(12L).findPartner(col, 1D));

        col = getUsers(manager, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L);
        assertEquals(manager.getUser(12L), manager.getUser(1L).findPartner(col, 1D));

        col = getUsers(manager, 6L, 7L, 8L);
        assertNull(manager.getUser(11L).findPartner(col, 1D));
        assertNull(manager.getUser(11L).findPartner(col, 100D));
    }

    @Test
    public void testFindPartnerEffectOfT() {
        double alpha = 1D;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L, 5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L));
        partitions.put(3L, initSet(10L, 11L, 12L, 13L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put(1L,  initSet(2L, 4L, 6L, 8L, 10L, 12L));
        friendships.put(2L,  initSet(3L, 6L, 9L, 12L));
        friendships.put(3L,  initSet(4L, 8L, 12L));
        friendships.put(4L,  initSet(5L, 10L));
        friendships.put(5L,  initSet(6L, 12L));
        friendships.put(6L,  initSet(7L));
        friendships.put(7L,  initSet(8L));
        friendships.put(8L,  initSet(9L));
        friendships.put(9L,  initSet(10L));
        friendships.put(10L, initSet(11L));
        friendships.put(11L, initSet(12L));
        friendships.put(12L, initSet(13L));
        friendships.put(13L, Collections.<Long>emptySet());

        JabejaManager manager = initGraph(alpha, 2D, .2D, 9, partitions, friendships);

        //Scenario 2: ensure that non-positive gain is impossible with T=1, and possible with T > 1
        Collection<JabejaUser> col = getUsers(manager, 6L, 7L, 8L, 9L);
        assertNull(manager.getUser(10L).findPartner(col, 1D));
        assertEquals(manager.getUser(9L), manager.getUser(10L).findPartner(col, 1.001D));
    }

    @Test
    public void testFindPartnerEffectOfAlpha() {
        //Taken directly from figure 2 of Ja-be-Ja: A Distributed Algorithm for Balanced Graph Partitioning
        //SICS Technical Report T2013:03
        //ISSN 1100-3154

        JabejaManager fig2aAlpha1     = initGraph(1D,     2D, .2D, 9, fig2aPartitions, fig2aFriendships);
        JabejaManager fig2aAlpha1_001 = initGraph(1.001D, 2D, .2D, 9, fig2aPartitions, fig2aFriendships);

        JabejaManager fig2bAlpha1     = initGraph(1D,     2D, .2D, 9, fig2bPartitions, fig2bFriendships);
        JabejaManager fig2bAlpha1_001 = initGraph(1.001D, 2D, .2D, 9, fig2bPartitions, fig2bFriendships);

        assertEquals(fig2aAlpha1.getUser    (7L), fig2aAlpha1.getUser    (2L).findPartner(getUsers(fig2aAlpha1,     7L), 1D));
        assertEquals(fig2aAlpha1.getUser    (2L), fig2aAlpha1.getUser    (7L).findPartner(getUsers(fig2aAlpha1,     2L), 1D));
        assertEquals(fig2aAlpha1_001.getUser(7L), fig2aAlpha1_001.getUser(2L).findPartner(getUsers(fig2aAlpha1_001, 7L), 1D));
        assertEquals(fig2aAlpha1_001.getUser(2L), fig2aAlpha1_001.getUser(7L).findPartner(getUsers(fig2aAlpha1_001, 2L), 1D));

        assertNull(fig2bAlpha1.getUser    (3L).findPartner(getUsers(fig2bAlpha1,     10L), 1D));
        assertNull(fig2bAlpha1.getUser    (10L).findPartner(getUsers(fig2bAlpha1,     3L), 1D));
        assertEquals(fig2bAlpha1_001.getUser(10L), fig2bAlpha1_001.getUser(3L).findPartner(getUsers(fig2bAlpha1_001, 10L), 1D));
        assertEquals(fig2bAlpha1_001.getUser(3L), fig2bAlpha1_001.getUser(10L).findPartner(getUsers(fig2bAlpha1_001, 3L), 1D));
    }
}
