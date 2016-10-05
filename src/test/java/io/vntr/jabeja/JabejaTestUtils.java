package io.vntr.jabeja;

import io.vntr.TestUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class JabejaTestUtils {
    public static JabejaManager initGraph(double alpha, double initialT, double deltaT, int k, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> friendships) {
        JabejaManager manager = new JabejaManager(alpha, initialT, deltaT, k);
        for(Long pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Long uid : partitions.get(pid)) {
                manager.addUser(new JabejaUser("User " + uid, uid, pid, alpha, manager));
            }
        }
        for (Long uid1 : friendships.keySet()) {
            for (Long uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static List<JabejaUser> getUsers(JabejaManager manager, Long... uids) {
        List<JabejaUser> list = new LinkedList<JabejaUser>();
        for(Long uid : uids) {
            list.add(manager.getUser(uid));
        }
        return list;
    }

    public static JabejaManager initGraph(double alpha, double initialT, double deltaT, int k, long numPartitions, Map<Long, Set<Long>> friendships) {
        Set<Long> pids = new HashSet<Long>();
        for(long pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Long, Set<Long>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
        return initGraph(alpha, initialT, deltaT, k, partitions, friendships);
    }
}
