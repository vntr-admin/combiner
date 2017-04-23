package io.vntr.spaja;

import io.vntr.InitUtils;
import io.vntr.TestUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.InitUtils.getUToMasterMap;
import static io.vntr.InitUtils.getUToReplicasMap;

/**
 * Created by robertlindquist on 10/3/16.
 */
public class SpajaTestUtils {
    public static SpajaManager initGraph(int minNumReplicas, float alpha, float initialT, float deltaT, int randomSampingSize, int numPartitions, Map<Integer, Set<Integer>> friendships) {
        Set<Integer> pids = new HashSet<>();
        for(int pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
        Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);
        return initGraph(minNumReplicas, alpha, initialT, deltaT, randomSampingSize, partitions, friendships, replicas);
    }

    public static SpajaManager initGraph(int minNumReplicas, float alpha, float initialT, float deltaT, int randomSampingSize, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicaPartitions) {
        SpajaManager manager = new SpajaManager(minNumReplicas, alpha, initialT, deltaT, randomSampingSize);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
        }

        Map<Integer, Integer> uToMasterMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uToReplicasMap = getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Integer uid : friendships.keySet()) {
            SpajaUser user = new SpajaUser(uid, alpha, minNumReplicas, manager);
            Integer pid = uToMasterMap.get(uid);
            user.setMasterPid(pid);

            manager.addUser(user, pid);

            for (Integer rPid : uToReplicasMap.get(uid)) {
                manager.addReplica(user, rPid);
            }
        }

        for(Integer uid : friendships.keySet()) {
            for(Integer friendId : friendships.get(uid)) {
                manager.befriend(manager.getUserMasterById(uid), manager.getUserMasterById(friendId));
            }
        }

        return manager;
    }
}
