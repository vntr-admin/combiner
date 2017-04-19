package io.vntr.j2;

import io.vntr.utils.ProbabilityUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class J2InitUtilsTest {

    @Test
    public void testInitGraph() {
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

        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        int expectedCut = 56; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3

        J2Manager manager = J2InitUtils.initGraph(1, 2, 0.5f, 15, 0, partitions, friendships);
        assertEquals(manager.getAllPartitionIds(), partitions.keySet());
        assertEquals(partitions, manager.getPartitionToUsers());
        assertEquals(bidirectionalFriendships, manager.getFriendships());
        assertTrue(manager.getMigrationTally() == 0);

        for(Integer pid : partitions.keySet()) {
            assertEquals(partitions.get(pid), manager.getPartition(pid));
            for(Integer uid : partitions.get(pid)) {
                J2User user = manager.getUser(uid);
                assertEquals(user.getPid(), pid);
                assertEquals(user.getFriendIDs(), bidirectionalFriendships.get(uid));
                assertEquals(uid, user.getId());
            }
        }

        assertTrue(manager.getEdgeCut() == expectedCut);
    }
}
