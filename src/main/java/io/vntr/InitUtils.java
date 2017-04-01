package io.vntr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class InitUtils {
    public static Map<Integer, Integer> getUToMasterMap(Map<Integer, Set<Integer>> partitions) {
        Map<Integer, Integer> map = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            for(Integer uid : partitions.get(pid)) {
                map.put(uid, pid);
            }
        }
        return map;
    }

    public static Map<Integer, Set<Integer>> getUToReplicasMap(Map<Integer, Set<Integer>> replicaPartitions, Set<Integer> allUids) {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for(Integer uid : allUids) {
            map.put(uid, new HashSet<Integer>());
        }
        for(Integer pid : replicaPartitions.keySet()) {
            for(Integer uid : replicaPartitions.get(pid)) {
                map.get(uid).add(pid);
            }
        }
        return map;
    }
}
