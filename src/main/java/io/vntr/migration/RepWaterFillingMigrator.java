package io.vntr.migration;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 6/27/17.
 */
public class RepWaterFillingMigrator {
    public static TIntIntMap getUserMigrationStrategy(Integer pid, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas) {
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        TIntSet masterIds = partitions.get(pid);
        TIntIntMap strategy = new TIntIntHashMap(masterIds.size() + 1);

        for(TIntIterator iter = masterIds.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            TIntSet replicaPids = uidToReplicasMap.get(uid);
            if(replicaPids.size() > 0) {
                int newPid = getRandomElement(uidToReplicasMap.get(uid));
                strategy.put(uid, newPid);
                iter.remove();
            }
        }

        WaterFillingPriorityQueue priorityQueue = new WaterFillingPriorityQueue(partitions, strategy, pid);

        for(TIntIterator iter = masterIds.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            int newPid = priorityQueue.getNextPid();
            strategy.put(uid, newPid);
        }

        return strategy;
    }

}
