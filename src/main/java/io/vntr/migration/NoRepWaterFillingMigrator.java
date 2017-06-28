package io.vntr.migration;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 6/27/17.
 */
public class NoRepWaterFillingMigrator {
    public static TIntIntMap migrateOffPartition(int pid, TIntObjectMap<TIntSet> partitions) {
        int[] uids = partitions.get(pid).toArray();
        shuffle(uids);
        TIntIntMap strategy = new TIntIntHashMap(uids.length+1);
        WaterFillingPriorityQueue priorityQueue = new WaterFillingPriorityQueue(partitions, new TIntIntHashMap(), pid);

        for(int uid : uids) {
            int newPid = priorityQueue.getNextPid();
            strategy.put(uid, newPid);
        }

        return strategy;
    }

}
