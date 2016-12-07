package io.vntr.metis;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 12/6/16.
 */
public class MetisTestUtils {
    public static MetisManager initGraph(Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        MetisManager manager = new MetisManager();
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new MetisUser(uid, pid));
            }
        }
        for (Integer uid1 : friendships.keySet()) {
            for (Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }
}
