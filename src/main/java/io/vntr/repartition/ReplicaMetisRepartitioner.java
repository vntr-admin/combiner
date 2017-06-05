package io.vntr.repartition;

import io.vntr.utils.Utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 6/4/17.
 */
public class ReplicaMetisRepartitioner {
    public static RepResults repartition(String commandLiteral, String tempDir, Map<Integer, Set<Integer>> friendships, Set<Integer> pids, int minNumReplicas) {

        Map<Integer, Integer> uidToPidMap = MetisRepartitioner.partition(commandLiteral, tempDir, friendships, pids);

        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        for(int pid : pids) {
            partitions.put(pid, new HashSet<Integer>());
        }
        for(Integer uid : uidToPidMap.keySet()) {
            int pid = uidToPidMap.get(uid);
            partitions.get(pid).add(uid);
        }

        Map<Integer, Set<Integer>> replicas = Utils.getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);

        Map<Integer, Set<Integer>> uidToReplicasMap = Utils.getUToReplicasMap(replicas, friendships.keySet());

        RepResults repResults = new RepResults(0, uidToPidMap, uidToReplicasMap);
        return repResults;
    }
}
