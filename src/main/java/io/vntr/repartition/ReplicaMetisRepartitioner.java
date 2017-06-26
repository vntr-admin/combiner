package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 6/4/17.
 */
public class ReplicaMetisRepartitioner {
    public static RepResults repartition(String commandLiteral, String tempDir, TShortObjectMap<TShortSet> friendships, TShortSet pids, short minNumReplicas) {

        TShortShortMap uidToPidMap = MetisRepartitioner.partition(commandLiteral, tempDir, friendships, pids);

        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>(pids.size()+1);
        for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
            partitions.put(iter.next(), new TShortHashSet());
        }
        for(short uid : uidToPidMap.keys()) {
            short pid = uidToPidMap.get(uid);
            partitions.get(pid).add(uid);
        }

        TShortObjectMap<TShortSet> replicas = getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);

        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        RepResults repResults = new RepResults(0, uidToPidMap, uidToReplicasMap);
        return repResults;
    }
}
