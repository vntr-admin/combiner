package io.vntr.hermes;

import io.vntr.TestUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesTestUtils {
    public static HermesManager initGraph(double gamma, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> friendships) {
        HermesManager manager = new HermesManager(gamma);
        for(Long pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Long uid : partitions.get(pid)) {
                manager.addUser(new HermesUser(uid, "User " + uid, pid, gamma, manager));
            }
        }
        for(Long uid1 : friendships.keySet()) {
            for(Long uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static HermesManager initGraph(double gamma, long numPartitions, Map<Long, Set<Long>> friendships) {
        Set<Long> pids = new HashSet<Long>();
        for(long pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Long, Set<Long>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());

        return initGraph(gamma, partitions, friendships);
    }
}
