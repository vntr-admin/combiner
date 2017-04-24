package io.vntr.sparmes;

import io.vntr.RepUser;

import java.util.Map;
import java.util.Set;

import static io.vntr.Utils.*;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class SparmesInitUtils {
    public static SparmesManager initGraph(int minNumReplicas, float gamma, int k, boolean probabilistic, double logicalMigrationRatio, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicaPartitions) {
        SparmesManager manager = new SparmesManager(minNumReplicas, gamma, k, probabilistic, logicalMigrationRatio);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
        }

        Map<Integer, Integer> uToMasterMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uToReplicasMap = getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Integer uid : friendships.keySet()) {
            Integer pid = uToMasterMap.get(uid);
            RepUser user = new RepUser(uid, pid);//, gamma, manager, manager.getMinNumReplicas());
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
