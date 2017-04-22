package io.vntr.replicadummy;

import java.util.*;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyMigrationStrategy {
    private ReplicaDummyManager manager;

    public ReplicaDummyMigrationStrategy(ReplicaDummyManager manager) {
        this.manager = manager;
    }

    public Map<Integer, Integer> getUserMigrationStrategy(Integer partitionId) {
        List<Integer> masterIds = new LinkedList<>(manager.getPartitionById(partitionId).getIdsOfMasters());
        Collections.shuffle(masterIds);
        Map<Integer, Integer> pWeightMap = getPWeightMap(partitionId);
        Map<Integer, Integer> strategy = new HashMap<>();

        for(int masterId : masterIds) {
            Integer minReplica = getLightestReplicaPid(masterId, pWeightMap);
            strategy.put(masterId, minReplica);
            pWeightMap.put(minReplica, pWeightMap.get(minReplica) + 1);
        }

        return strategy;
    }

    Map<Integer, Integer> getPWeightMap(Integer partitionId) {
        Map<Integer, Integer> pWeightMap = new HashMap<>();
        for(int pid : manager.getPids()) {
            if(pid != partitionId) {
                pWeightMap.put(pid, manager.getPartitionById(pid).getNumMasters());
            }
        }
        return pWeightMap;
    }

    private Integer getLightestReplicaPid(int masterId, Map<Integer, Integer> pWeightMap) {
        Integer minReplica = null;
        int minWeight = Integer.MAX_VALUE;
        for(int replicaPid : manager.getUserMasterById(masterId).getReplicaPids()) {
            int weight = pWeightMap.get(replicaPid);
            if(weight < minWeight) {
                minWeight = weight;
                minReplica = replicaPid;
            }
        }
        return minReplica;
    }

}
