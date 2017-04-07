package io.vntr.sparmes;

import java.util.Map;
import java.util.Set;

import static io.vntr.InitUtils.getUToMasterMap;
import static io.vntr.InitUtils.getUToReplicasMap;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class SparmesInitUtils {
    public static SparmesManager initGraph(int minNumReplicas, float gamma, int k, boolean probabilistic, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicaPartitions) {
        SparmesManager manager = new SparmesManager(minNumReplicas, gamma, k, probabilistic);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
        }

        Map<Integer, Integer> uToMasterMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uToReplicasMap = getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Integer uid : friendships.keySet()) {
            Integer pid = uToMasterMap.get(uid);
            SparmesUser user = new SparmesUser(uid, pid, gamma, manager, manager.getMinNumReplicas());
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
