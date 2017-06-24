package io.vntr.middleware;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.manager.NoRepManager;
import io.vntr.utils.ProbabilityUtils;
import io.vntr.utils.TroveUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static io.vntr.utils.InitUtils.initNoRepManager;
import static io.vntr.utils.TroveUtils.convert;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesMiddlewareTest {

    @Test
    public void testRemovePartition() {
        int numUsers = 1000;
        int numPartitions = 10;

        float gamma = 1.5f;
        TIntObjectMap<TIntSet> friendships = convert(getTopographyForMultigroupSocialNetwork(numUsers, 12, 0.1f, 0.1f));
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        for(int pid=0; pid<numPartitions; pid++) {
            partitions.put(pid, new TIntHashSet());
        }
        for(int uid=0; uid<numUsers; uid++) {
            partitions.get(uid % numPartitions).add(uid);
        }
        NoRepManager manager = initNoRepManager(0, false, convert(partitions), convert(friendships));
        HermesMiddleware middleware = new HermesMiddleware(gamma, 3, 100, manager);
        middleware.removePartition(ProbabilityUtils.getKDistinctValuesBetweenMandNInclusive(1, 0, (numPartitions - 1)).iterator().next());
        for(Integer uid : friendships.keys()) {
            assertEquals(manager.getUser(uid).getFriendIDs(), friendships.get(uid));
        }

        TIntObjectMap<TIntSet> finalTopology = manager.getPartitionToUsers();
        TIntSet observedUsers = new TIntHashSet();
        for(Integer pid : finalTopology.keys()) {
            observedUsers.addAll(finalTopology.get(pid));
        }

        assertEquals(friendships.keySet(), observedUsers);

        middleware.repartition(); //Just making sure this doesn't throw an NPE or anything
    }

}
