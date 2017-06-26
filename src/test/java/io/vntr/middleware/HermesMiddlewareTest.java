package io.vntr.middleware;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
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
        TShortObjectMap<TShortSet> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, intraGroupFriendshipProb);
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        for(short pid=0; pid<numPartitions; pid++) {
            partitions.put(pid, new TShortHashSet());
        }
        for(short uid=0; uid<numUsers; uid++) {
            partitions.get((short)(uid % numPartitions)).add(uid);
        }
        NoRepManager manager = initNoRepManager(0, false, partitions, friendships);
        HermesMiddleware middleware = new HermesMiddleware(gamma, (short)3, (short)100, manager);
        int partitionToRemove = (int)(Math.random() * numPartitions);
        middleware.removePartition((short)(Math.random() * numPartitions));
        for(short uid : friendships.keys()) {
            assertEquals(manager.getUser(uid).getFriendIDs(), friendships.get(uid));
        }

        TShortObjectMap<TShortSet> finalTopology = manager.getPartitionToUsers();
        TShortSet observedUsers = new TShortHashSet();
        for(short pid : finalTopology.keys()) {
            observedUsers.addAll(finalTopology.get(pid));
        }

        assertEquals(friendships.keySet(), observedUsers);

        middleware.repartition(); //Just making sure this doesn't throw an NPE or anything
    }

}
