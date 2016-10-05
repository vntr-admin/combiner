package io.vntr.hermes;

import io.vntr.utils.ProbabilityUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesMiddlewareTest {

    @Test
    public void testRemovePartition() {
        int numUsers = 1000;
        int numPartitions = 10;

        double gamma = 1.5;
        Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, 12, 0.1, 0.1);
        Map<Integer, Set<Integer>> partitions = new HashMap<Integer, Set<Integer>>();
        for(int pid=0; pid<numPartitions; pid++) {
            partitions.put(pid, new HashSet<Integer>());
        }
        for(int uid=0; uid<numUsers; uid++) {
            partitions.get(uid % numPartitions).add(uid);
        }
        HermesManager manager = HermesTestUtils.initGraph(gamma, partitions, friendships);
        HermesMiddleware middleware = new HermesMiddleware(manager, gamma);
        middleware.removePartition(ProbabilityUtils.getKDistinctValuesBetweenMandNInclusive(1, 0, (int) (numPartitions - 1)).iterator().next());
        for(Integer uid : friendships.keySet()) {
            assertEquals(manager.getUser(uid).getFriendIDs(), friendships.get(uid));
        }

        Map<Integer, Set<Integer>> finalTopology = manager.getPartitionToUserMap();
        Set<Integer> observedUsers = new HashSet<Integer>();
        for(Integer pid : finalTopology.keySet()) {
            observedUsers.addAll(finalTopology.get(pid));
        }

        assertEquals(friendships.keySet(), observedUsers);

        manager.repartition(); //Just making sure this doesn't throw an NPE or anything
    }

}
