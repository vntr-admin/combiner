package io.vntr.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class Utils {

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

    public static Map<Integer, Integer> getPToFriendCount(Integer uid, Map<Integer, Set<Integer>> friendships, Map<Integer, Integer> uidToPidMap, Set<Integer> pids) {
        Map<Integer, Integer> pToFriendCount = new HashMap<>();
        for(Integer pid : pids) {
            pToFriendCount.put(pid, 0);
        }
        for(Integer friendId : friendships.get(uid)) {
            Integer pid = uidToPidMap.get(friendId);
            pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
        }
        return pToFriendCount;
    }

    public static Map<Integer, Integer> getUserCounts(Map<Integer, Set<Integer>> partitions) {
        Map<Integer, Integer> map = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            map.put(pid, partitions.get(pid).size());
        }
        return map;
    }

    public static Map<Integer, Set<Integer>> generateBidirectionalFriendshipSet(Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> bidirectionalFriendshipSet = new HashMap<>();
        for(Integer uid : friendships.keySet()) {
            bidirectionalFriendshipSet.put(uid, new HashSet<Integer>());
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                bidirectionalFriendshipSet.get(uid1).add(uid2);
                bidirectionalFriendshipSet.get(uid2).add(uid1);
            }
        }
        return bidirectionalFriendshipSet;
    }
}