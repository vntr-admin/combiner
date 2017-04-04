package io.vntr.jabeja;

import io.vntr.TestUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class JabejaTestUtils {
    public static JabejaManager initGraph(float alpha, float initialT, float deltaT, float befriendInitialT, float befriendDeltaT, int k, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        JabejaManager manager = new JabejaManager(alpha, initialT, deltaT, befriendInitialT, befriendDeltaT, k);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new JabejaUser(uid, pid, alpha, manager));
            }
        }
        for (Integer uid1 : friendships.keySet()) {
            for (Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static List<JabejaUser> getUsers(JabejaManager manager, Integer... uids) {
        List<JabejaUser> list = new LinkedList<>();
        for(Integer uid : uids) {
            list.add(manager.getUser(uid));
        }
        return list;
    }

    public static JabejaManager initGraph(float alpha, float initialT, float deltaT, float befriendInitialT, float befriendDeltaT, int k, int numPartitions, Map<Integer, Set<Integer>> friendships) {
        Set<Integer> pids = new HashSet<>();
        for(int pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
        return initGraph(alpha, initialT, deltaT, befriendInitialT, befriendDeltaT, k, partitions, friendships);
    }
}
