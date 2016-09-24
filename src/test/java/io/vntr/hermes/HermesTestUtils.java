package io.vntr.hermes;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesTestUtils {
    public static HermesManager initGraph(double gamma, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> friendships) {
        HermesManager manager = new HermesManager(0.9D);
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
}
