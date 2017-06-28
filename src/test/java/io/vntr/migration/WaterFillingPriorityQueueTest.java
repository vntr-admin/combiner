package io.vntr.migration;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.initSet;

/**
 * Created by robertlindquist on 6/27/17.
 */
public class WaterFillingPriorityQueueTest {

    @Test
    public void testSomething() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20));
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));
        friendships.put( 4, initSet( 5, 10, 15));
        friendships.put( 5, initSet( 6, 12, 18));
        friendships.put( 6, initSet( 7, 14));
        friendships.put( 7, initSet( 8, 16));
        friendships.put( 8, initSet( 9, 18));
        friendships.put( 9, initSet(10, 20));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, initSet(14));
        friendships.put(14, initSet(15));
        friendships.put(15, initSet(16));
        friendships.put(16, initSet(17));
        friendships.put(17, initSet(18));
        friendships.put(18, initSet(19));
        friendships.put(19, initSet(20));
        friendships.put(20, new TIntHashSet());

        TIntObjectMap<TIntSet> replicaPartitions = new TIntObjectHashMap<>();
        replicaPartitions.put(1, initSet(16, 17, 18, 19, 20));
        replicaPartitions.put(2, initSet( 1,  2,  3,  4,  5, 19));
        replicaPartitions.put(3, initSet( 6,  7,  8,  9, 10,  2, 17));
        replicaPartitions.put(4, initSet(11, 12, 13, 14, 15,  3));

        WaterFillingPriorityQueue queue = new WaterFillingPriorityQueue(partitions, new TIntIntHashMap(), 4);
        //TODO: do this
    }
}
