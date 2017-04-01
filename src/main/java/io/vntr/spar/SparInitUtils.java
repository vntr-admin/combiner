package io.vntr.spar;

import java.util.Map;
import java.util.Set;

import static io.vntr.InitUtils.getUToMasterMap;
import static io.vntr.InitUtils.getUToReplicasMap;

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
            SparUser user = new SparUser(uid);
            Integer pid = uToMasterMap.get(uid);
            user.setMasterPartitionId(pid);
            user.setPartitionId(pid);

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
