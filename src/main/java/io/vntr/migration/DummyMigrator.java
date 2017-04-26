package io.vntr.migration;

import java.util.*;

import static io.vntr.utils.Utils.getUserCounts;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class DummyMigrator {
    public static Map<Integer, Integer> getUserMigrationStrategy(Integer partitionId, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        List<Integer> masterIds = new LinkedList<>(partitions.get(partitionId));
        Collections.shuffle(masterIds);

        Map<Integer, Integer> pWeightMap = getUserCounts(partitions);
        pWeightMap.remove(partitionId);

        Map<Integer, Integer> strategy = new HashMap<>();

        for(int masterId : masterIds) {
            Integer minReplica = getLightestReplicaPid(replicas.get(masterId), pWeightMap);
            strategy.put(masterId, minReplica);
            pWeightMap.put(minReplica, pWeightMap.get(minReplica) + 1);
        }

        return strategy;
    }

    private static Integer getLightestReplicaPid(Set<Integer> replicaPids, Map<Integer, Integer> pWeightMap) {
        Integer minReplica = null;
        int minWeight = Integer.MAX_VALUE;
        for(int replicaPid : replicaPids) {
            int weight = pWeightMap.get(replicaPid);
            if(weight < minWeight) {
                minWeight = weight;
                minReplica = replicaPid;
            }
        }
        return minReplica;
    }

}
