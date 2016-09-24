package io.vntr.jabeja;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaUserTest {

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
    public void testFindPartner() {
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

        manager.befriend(1L, 4L);
        manager.befriend(1L, 5L);
        manager.befriend(1L, 6L);

        JabejaUser partner = user1.findPartner(Arrays.asList(user2, user3, user4), 1);
        assertEquals(partner, user4);
    }

    @Test
    public void testFindPartner2() {
        double alpha = 1D;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, new HashSet<Long>(Arrays.asList( 1L,  2L,  3L,  4L, 5L)));
        partitions.put(2L, new HashSet<Long>(Arrays.asList( 6L,  7L,  8L,  9L)));
        partitions.put(3L, new HashSet<Long>(Arrays.asList(10L, 11L, 12L, 13L)));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put(1L,  new HashSet<Long>(Arrays.asList(2L, 4L, 6L, 8L, 10L, 12L)));
        friendships.put(2L,  new HashSet<Long>(Arrays.asList(3L, 6L, 9L, 12L)));
        friendships.put(3L,  new HashSet<Long>(Arrays.asList(4L, 8L, 12L)));
        friendships.put(4L,  new HashSet<Long>(Arrays.asList(5L, 10L)));
        friendships.put(5L,  new HashSet<Long>(Arrays.asList(6L, 12L)));
        friendships.put(6L,  new HashSet<Long>(Arrays.asList(7L)));
        friendships.put(7L,  new HashSet<Long>(Arrays.asList(8L)));
        friendships.put(8L,  new HashSet<Long>(Arrays.asList(9L)));
        friendships.put(9L,  new HashSet<Long>(Arrays.asList(10L)));
        friendships.put(10L, new HashSet<Long>(Arrays.asList(11L)));
        friendships.put(11L, new HashSet<Long>(Arrays.asList(12L)));
        friendships.put(12L, new HashSet<Long>(Arrays.asList(13L)));

        JabejaManager manager = JabejaTestUtils.initGraph(alpha, 2D, .2D, 9, partitions, friendships);

        //Scenarios:
        //    2: ensure that negative gain is impossible with T=1, and possible with T > 1
        //    3: make sure alpha does whatever it's supposed to do


        //Scenario 1: ensure that the best partner is chosen
        Collection<JabejaUser> col = JabejaTestUtils.getUsers(manager, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertEquals(manager.getUser(1L), manager.getUser(12L).findPartner(col, 1D));

        col = JabejaTestUtils.getUsers(manager, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
        assertEquals(manager.getUser(2L), manager.getUser(12L).findPartner(col, 1D));

        col = JabejaTestUtils.getUsers(manager, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L);
        assertEquals(manager.getUser(12L), manager.getUser(1L).findPartner(col, 1D));

        col = JabejaTestUtils.getUsers(manager, 6L, 7L, 8L);
        assertNull(manager.getUser(11L).findPartner(col, 1D));
        assertNull(manager.getUser(11L).findPartner(col, 100D));

        //Scenario 2: ensure that non-positive gain is impossible with T=1, and possible with T > 1
        col = JabejaTestUtils.getUsers(manager, 6L, 7L, 8L, 9L);
        assertNull(manager.getUser(10L).findPartner(col, 1D));
        assertEquals(manager.getUser(9L), manager.getUser(10L).findPartner(col, 1.001D));

        //Scenario 3: make sure alpha does whatever it's supposed to do
        //TODO: do this
    }
}
