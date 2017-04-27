package io.vntr.utils;

import io.vntr.User;
import io.vntr.manager.NoRepManager;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.Utils.generateBidirectionalFriendshipSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class InitUtilsTest {

    @Test
    public void testInitSManager() {
        //TODO: do this
    }

    @Test
    public void testInitHManager() {
        //TODO: do this
    }

    @Test
    public void testInitJ2Manager() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        int expectedCut = 56; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);
        assertEquals(manager.getPids(), partitions.keySet());
        assertEquals(partitions, manager.getPartitionToUsers());
        assertEquals(bidirectionalFriendships, manager.getFriendships());
        assertTrue(manager.getMigrationTally() == 0);

        for(Integer pid : partitions.keySet()) {
            assertEquals(partitions.get(pid), manager.getPartition(pid));
            for(Integer uid : partitions.get(pid)) {
                User user = manager.getUser(uid);
                assertEquals(user.getBasePid(), pid);
                assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
                assertEquals(uid, user.getId());
            }
        }

        assertTrue(manager.getEdgeCut() == expectedCut);
    }
}
