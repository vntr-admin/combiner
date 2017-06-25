package io.vntr.middleware;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.manager.NoRepManager;
import org.junit.Test;

import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static io.vntr.utils.InitUtils.initNoRepManager;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesMiddlewareTest {

    @Test
    public void testRemovePartition() {
        int numUsers = 1000;
        int numPartitions = 10;
        int numGroups = 12;
        float groupProb = 0.1f;
        float intraGroupFriendshipProb = 0.1f;

        float gamma = 1.5f;
        TIntObjectMap<TIntSet> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, intraGroupFriendshipProb);
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        for(int pid=0; pid<numPartitions; pid++) {
            partitions.put(pid, new TIntHashSet());
        }
        for(int uid=0; uid<numUsers; uid++) {
            partitions.get(uid % numPartitions).add(uid);
        }
        NoRepManager manager = initNoRepManager(0, false, partitions, friendships);
        HermesMiddleware middleware = new HermesMiddleware(gamma, 3, 100, manager);
        int partitionToRemove = (int)(Math.random() * numPartitions);
        middleware.removePartition((int)(Math.random() * numPartitions));
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
