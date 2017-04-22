package io.vntr.hermes;

import io.vntr.User;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class HermesInitUtils {
    public static HermesManager initGraph(float gamma, int k, float maxIterationToNumUsersRatio, double logicalMigrationRatio, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        HermesManager manager = new HermesManager(gamma, maxIterationToNumUsersRatio, k, logicalMigrationRatio);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new User(uid, pid));
            }
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }
}
