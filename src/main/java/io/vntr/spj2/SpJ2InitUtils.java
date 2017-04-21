package io.vntr.spj2;

import io.vntr.RepUser;

import java.util.Map;
import java.util.Set;

import static io.vntr.InitUtils.getUToMasterMap;
import static io.vntr.InitUtils.getUToReplicasMap;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class SpJ2InitUtils {
    public static SpJ2Manager initGraph(int minNumReplicas, float alpha, float initialT, float deltaT, int k, double logicalMigrationRatio, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicaPartitions) {
        SpJ2Manager manager = new SpJ2Manager(minNumReplicas, alpha, initialT, deltaT, k, logicalMigrationRatio);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
        }

        Map<Integer, Integer> uToMasterMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uToReplicasMap = getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Integer uid : friendships.keySet()) {
            RepUser user = new RepUser(uid);
            Integer pid = uToMasterMap.get(uid);
            user.setBasePid(pid);

            manager.addUser(user, pid);
        }

        for(Integer uid : friendships.keySet()) {
            for (Integer rPid : uToReplicasMap.get(uid)) {
                manager.addReplica(manager.getUserMasterById(uid), rPid);
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
