package io.vntr.utils;

import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.manager.RepManager;

import java.util.*;

import static io.vntr.utils.Utils.*;

/**
 * Created by robertlindquist on 4/26/17.
 */
public class InitUtils {
    public static NoRepManager initNoRepManager(double logicalMigrationRatio, boolean placeNewUserRandomly, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        NoRepManager manager = new NoRepManager(logicalMigrationRatio, placeNewUserRandomly);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new User(uid, pid));
            }
        }
        for (Integer uid1 : friendships.keySet()) {
            for (Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static RepManager initRepManager(int minNumReplicas, double logicalMigrationRatio, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicaPartitions) {
        RepManager manager = new RepManager(minNumReplicas, logicalMigrationRatio);
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
                manager.addReplica(manager.getUserMaster(uid), rPid);
            }
        }

        for(Integer uid : friendships.keySet()) {
            for(Integer friendId : friendships.get(uid)) {
                manager.befriend(manager.getUserMaster(uid), manager.getUserMaster(friendId));
            }
        }

        return manager;
    }
}
