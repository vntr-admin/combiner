package io.vntr.hermar;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class HermarInitUtils {
    public static HermarManager initGraph(float gamma, boolean probabilistic, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        HermarManager manager = new HermarManager(gamma, probabilistic);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new HermarUser(uid, pid, gamma, manager));
            }
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static HermarManager initGraph(float gamma, int k, float maxIterationToNumUsersRatio, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        HermarManager manager = new HermarManager(gamma, maxIterationToNumUsersRatio, k);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new HermarUser(uid, pid, gamma, manager));
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
