package io.vntr.spaja;

import io.vntr.TestUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 10/3/16.
 */
public class SparjaTestUtils {
    public static SpajaManager initGraph(int minNumReplicas, double alpha, double initialT, double deltaT, long numPartitions, Map<Long, Set<Long>> friendships) {
        SpajaManager manager = new SpajaManager(minNumReplicas, alpha, initialT, deltaT);
        for(long pid = 0; pid < numPartitions; pid++) {
            manager.addPartition(pid);
        }
        Map<Long, Set<Long>> partitions = TestUtils.getRandomPartitioning(manager.getAllPartitionIds(), friendships.keySet());
        Map<Long, Set<Long>> replicas = TestUtils.getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);
        return initGraph(minNumReplicas, alpha, initialT, deltaT, partitions, friendships, replicas);
    }

    public static SpajaManager initGraph(int minNumReplicas, double alpha, double initialT, double deltaT, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> friendships, Map<Long, Set<Long>> replicaPartitions) {
        SpajaManager manager = new SpajaManager(minNumReplicas, alpha, initialT, deltaT);
        for(Long pid : partitions.keySet()) {
            manager.addPartition(pid);
        }

        Map<Long, Long> uToMasterMap = getUToMasterMap(partitions);
        Map<Long, Set<Long>> uToReplicasMap = getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Long uid : friendships.keySet()) {
            SpajaUser user = new SpajaUser("User " + uid, uid, alpha, minNumReplicas, manager);
            Long pid = uToMasterMap.get(uid);
            user.setMasterPartitionId(pid);
            user.setPartitionId(pid);

            manager.addUser(user, pid);

            for (Long rPid : uToReplicasMap.get(uid)) {
                manager.addReplica(user, rPid);
            }
        }

        for(Long uid : friendships.keySet()) {
            for(Long friendId : friendships.get(uid)) {
                manager.befriend(manager.getUserMasterById(uid), manager.getUserMasterById(friendId));
            }
        }

        return manager;
    }

    private static Map<Long, Long> getUToMasterMap(Map<Long, Set<Long>> partitions) {
        Map<Long, Long> map = new HashMap<Long, Long>();
        for(Long pid : partitions.keySet()) {
            for(Long uid : partitions.get(pid)) {
                map.put(uid, pid);
            }
        }
        return map;
    }

    private static Map<Long, Set<Long>> getUToReplicasMap(Map<Long, Set<Long>> replicaPartitions, Set<Long> allUids) {
        Map<Long, Set<Long>> map = new HashMap<Long, Set<Long>>();
        for(Long uid : allUids) {
            map.put(uid, new HashSet<Long>());
        }
        for(Long pid : replicaPartitions.keySet()) {
            for(Long uid : replicaPartitions.get(pid)) {
                map.get(uid).add(pid);
            }
        }
        return map;
    }
}
