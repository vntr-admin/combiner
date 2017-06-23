package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.utils.TroveUtils;
import io.vntr.utils.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 6/4/17.
 */
public class ReplicaMetisRepartitioner {
    public static RepResults repartition(String commandLiteral, String tempDir, TIntObjectMap<TIntSet> friendships, TIntSet pids, int minNumReplicas) {

        Map<Integer, Integer> uidToPidMap = MetisRepartitioner.partition(commandLiteral, tempDir, friendships, pids);

        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
            partitions.put(iter.next(), new TIntHashSet());
        }
        for(Integer uid : uidToPidMap.keySet()) {
            int pid = uidToPidMap.get(uid);
            partitions.get(pid).add(uid);
        }

        TIntObjectMap<TIntSet> replicas = TroveUtils.getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);

        TIntObjectMap<TIntSet> uidToReplicasMap = TroveUtils.getUToReplicasMap(replicas, friendships.keySet());

        RepResults repResults = new RepResults(0, uidToPidMap, TroveUtils.convertTIntObjectMapTIntSetToMapSet(uidToReplicasMap));
        return repResults;
    }
}
