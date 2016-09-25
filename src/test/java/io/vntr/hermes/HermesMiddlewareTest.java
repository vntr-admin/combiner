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
        int numUsers = 2000;
        int numPartitions = 10;

        double gamma = 1.5;
        Map<Long, Set<Long>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, 20, 0.1, 0.1);
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        for(long pid=0L; pid<numPartitions; pid++) {
            partitions.put(pid, new HashSet<Long>());
        }
        for(long uid=0L; uid<numUsers; uid++) {
            partitions.get(uid % numPartitions).add(uid);
        }
        HermesManager manager = HermesTestUtils.initGraph(gamma, partitions, friendships);
        HermesMiddleware middleware = new HermesMiddleware(manager, gamma);
        middleware.removePartition(ProbabilityUtils.getKDistinctValuesBetweenMandNInclusive(1L, 0L, (long) (numPartitions - 1)).iterator().next());
        for(Long uid : friendships.keySet()) {
            assertEquals(manager.getUser(uid).getFriendIDs(), friendships.get(uid));
        }

        Map<Long, Set<Long>> finalTopology = manager.getPartitionToUserMap();
        Set<Long> observedUsers = new HashSet<Long>();
        for(Long pid : finalTopology.keySet()) {
            observedUsers.addAll(finalTopology.get(pid));
        }

        assertEquals(friendships.keySet(), observedUsers);

        manager.repartition(); //Just making sure this doesn't throw an NPE or anything
    }

}
