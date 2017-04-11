package io.vntr.jabar;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class JabarInitUtils {
    public static JabarManager initGraph(float alpha, float initialT, float deltaT, float befriendInitialT, float befriendDeltaT, int k, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        JabarManager manager = new JabarManager(alpha, initialT, deltaT, befriendInitialT, befriendDeltaT, k);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new JabarUser(uid, pid, alpha, manager));
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
