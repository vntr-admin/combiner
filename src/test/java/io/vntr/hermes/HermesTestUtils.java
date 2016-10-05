package io.vntr.hermes;

import io.vntr.TestUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesTestUtils {
    public static HermesManager initGraph(float gamma, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        HermesManager manager = new HermesManager(gamma);
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

    public static HermesManager initGraph(float gamma, int numPartitions, Map<Integer, Set<Integer>> friendships) {
        Set<Integer> pids = new HashSet<Integer>();
        for(int pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());

        return initGraph(gamma, partitions, friendships);
    }
}
