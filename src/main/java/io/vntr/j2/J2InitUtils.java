package io.vntr.j2;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2InitUtils {
    public static J2Manager initGraph(float alpha, float initialT, float deltaT, int k, double logicalMigrationRatio, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        J2Manager manager = new J2Manager(alpha, initialT, deltaT, k, logicalMigrationRatio);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new J2User(uid, pid));
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
