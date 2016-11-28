package io.vntr.dummy;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class DummyTestUtils {
    public static DummyManager initGraph(Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        DummyManager manager = new DummyManager();
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new DummyUser(uid, pid));
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
