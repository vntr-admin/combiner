package io.vntr.hermes;

import io.vntr.TestUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesTestUtils {
    public static HermesManager initGraph(float gamma, boolean probabilistic, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        HermesManager manager = new HermesManager(gamma, probabilistic);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new HermesUser(uid, pid, gamma, manager));
            }
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static HermesManager initGraph(float gamma, int k, float maxIterationToNumUsersRatio, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        HermesManager manager = new HermesManager(gamma, maxIterationToNumUsersRatio, k);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new HermesUser(uid, pid, gamma, manager));
            }
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static HermesManager initGraph(float gamma, boolean probabilistic, int numPartitions, Map<Integer, Set<Integer>> friendships) {
        Set<Integer> pids = new HashSet<>();
        for(int pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());

        return initGraph(gamma, probabilistic, partitions, friendships);
    }
}
