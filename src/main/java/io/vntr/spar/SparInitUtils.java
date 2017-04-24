package io.vntr.spar;

import io.vntr.RepUser;

import java.util.Map;
import java.util.Set;

import static io.vntr.Utils.*;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class SparInitUtils {
    public static SparManager initGraph(int minNumReplicas, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicaPartitions) {
        SparManager manager = new SparManager(minNumReplicas);
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
