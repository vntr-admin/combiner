package io.vntr.spaja;

import java.util.Map;
import java.util.Set;

import static io.vntr.Utils.*;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class SpajaInitUtils {
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
