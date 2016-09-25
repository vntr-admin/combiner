package io.vntr.hermes;

import io.vntr.TestUtils;
import io.vntr.User;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class HermesManagerTest {

    @Test
    public void testThingsInGeneral() {
        double gamma = 1.6D;
        HermesManager manager = new HermesManager(gamma);

        Long pid1 = manager.addPartition();
        Long pid2 = manager.addPartition();

        HermesUser user1 = new HermesUser(1L, "User 1", pid1, gamma, manager);
        HermesUser user2 = new HermesUser(2L, "User 2", pid2, gamma, manager);
        HermesUser user3 = new HermesUser(3L, "User 3", pid1, gamma, manager);
        HermesUser user4 = new HermesUser(4L, "User 4", pid2, gamma, manager);
        HermesUser user5 = new HermesUser(5L, "User 5", pid1, gamma, manager);
        HermesUser user6 = new HermesUser(6L, "User 6", pid2, gamma, manager);

        manager.addUser(user1);
        manager.addUser(user2);
        manager.addUser(user3);
        manager.addUser(user4);
        manager.addUser(user5);
        manager.addUser(user6);

        Map<Long, Set<Long>> pMap = manager.getPartitionToUserMap();
        assertTrue(pMap.size() == 2);
        assertTrue(pMap.get(pid1).equals(new HashSet<Long>(Arrays.asList(1L, 3L, 5L))));
        assertTrue(pMap.get(pid2).equals(new HashSet<Long>(Arrays.asList(2L, 4L, 6L))));

        assertEquals(user1, manager.getUser(1L));
        assertEquals(user2, manager.getUser(2L));
        assertEquals(user3, manager.getUser(3L));
        assertEquals(user4, manager.getUser(4L));
        assertEquals(user5, manager.getUser(5L));
        assertEquals(user6, manager.getUser(6L));

        manager.befriend(1L, 2L);
        manager.befriend(1L, 3L);
        manager.befriend(1L, 4L);
        manager.befriend(1L, 5L);
        manager.befriend(1L, 6L);

        assertTrue(manager.getEdgeCut() == 3L);

        manager.unfriend(1L, 6L);

        assertTrue(manager.getEdgeCut() == 2L);

        Long pid3 = manager.addPartition();

        HermesUser user7 = new HermesUser(7L, "User 7", pid3, gamma, manager);
        HermesUser user8 = new HermesUser(8L, "User 8", pid3, gamma, manager);
        HermesUser user9 = new HermesUser(9L, "User 9", pid3, gamma, manager);

        manager.addUser(user7);
        manager.addUser(user8);
        manager.addUser(user9);

        manager.befriend(1L, 7L);
        manager.befriend(1L, 8L);
        manager.befriend(1L, 9L);

        assertTrue(manager.getEdgeCut() == 5L);

        manager.removeUser(8L);

        assertTrue(manager.getEdgeCut() == 4L);

        manager.moveUser(4L, pid1);

        assertTrue(manager.getEdgeCut() == 3L);

        manager.moveUser(4L, pid3);

        assertTrue(manager.getEdgeCut() == 4L);

        for(long id=2L; id<=9L; id++) {
            if(id != 8L) {
                manager.moveUser(id, pid3);
            }
        }

        manager.repartition();

        System.out.println("Hermes edge cut #1: " + manager.getEdgeCut());
    }

    @Test
    public void testRepartitionInDepth() {
        //TODO: this occasionally doesn't terminate
        int numUsers = 5000;
        int numPartitions = 10;

        double gamma = 1.5;
        Map<Long, Set<Long>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, 30, 0.1, 0.1);
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        for(long pid=0L; pid<=numPartitions; pid++) {
            partitions.put(pid, new HashSet<Long>());
        }
        for(long uid=0L; uid<=numUsers; uid++) {
            partitions.get(uid % numPartitions).add(uid);
        }
        HermesManager manager = HermesTestUtils.initGraph(gamma, partitions, friendships);
        Long initialEdgeCut = manager.getEdgeCut();
        manager.repartition();
        Long finalEdgeCut = manager.getEdgeCut();
        System.out.println("Edge cut before: " + initialEdgeCut + ", after: " + finalEdgeCut);
    }
}
