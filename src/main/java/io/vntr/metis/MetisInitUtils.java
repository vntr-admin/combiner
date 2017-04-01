package io.vntr.metis;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class MetisInitUtils {
    public static MetisManager initGraph(Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, String gpmetisLocation, String gpmetisTempdir) {
        MetisManager manager = new MetisManager(gpmetisLocation, gpmetisTempdir);
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
