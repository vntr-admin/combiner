package io.vntr.jabeja;

import org.junit.Test;

import java.util.Arrays;

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

        //TODO: more testing
    }
}
