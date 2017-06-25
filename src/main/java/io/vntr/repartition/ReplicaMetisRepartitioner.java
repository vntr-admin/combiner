package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 6/4/17.
 */
public class ReplicaMetisRepartitioner {
    public static RepResults repartition(String commandLiteral, String tempDir, TIntObjectMap<TIntSet> friendships, TIntSet pids, int minNumReplicas) {

        TIntIntMap uidToPidMap = MetisRepartitioner.partition(commandLiteral, tempDir, friendships, pids);

        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>(pids.size()+1);
        for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
            partitions.put(iter.next(), new TIntHashSet());
        }
        for(Integer uid : uidToPidMap.keys()) {
            int pid = uidToPidMap.get(uid);
            partitions.get(pid).add(uid);
        }

        TIntObjectMap<TIntSet> replicas = getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);

        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        RepResults repResults = new RepResults(0, uidToPidMap, uidToReplicasMap);
        return repResults;
    }
}
