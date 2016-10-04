package io.vntr.sparmes;

import io.vntr.TestUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 10/3/16.
 */
public class SparmesTestUtils {

    public static SparmesManager initGraph(int minNumReplicas, double gamma, long numPartitions, Map<Long, Set<Long>> friendships) {
        Set<Long> pids = new HashSet<Long>();
        for(long pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Long, Set<Long>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
        Map<Long, Set<Long>> replicas = TestUtils.getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);
        return initGraph(minNumReplicas, gamma, partitions, friendships, replicas);
    }

    public static SparmesManager initGraph(int minNumReplicas, double gamma, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> friendships, Map<Long, Set<Long>> replicaPartitions) {
        SparmesManager manager = new SparmesManager(minNumReplicas, gamma);
        for(Long pid : partitions.keySet()) {
            manager.addPartition(pid);
        }

        Map<Long, Long> uToMasterMap = getUToMasterMap(partitions);
        Map<Long, Set<Long>> uToReplicasMap = getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Long uid : friendships.keySet()) {
            SparmesUser user = new SparmesUser("User " + uid, uid, gamma, manager);
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
