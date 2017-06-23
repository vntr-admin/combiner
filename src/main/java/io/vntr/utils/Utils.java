package io.vntr.utils;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

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

    public static Map<Integer, Set<Integer>> getInitialReplicasObeyingKReplication(int minNumReplicas, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            replicas.put(pid, new HashSet<Integer>());
        }

        Map<Integer, Set<Integer>> replicaLocations = new HashMap<>();
        for(Integer uid : friendships.keySet()) {
            replicaLocations.put(uid, new HashSet<Integer>());
        }

        Map<Integer, Integer> uMap = getUToMasterMap(partitions);

        //Step 1: add replicas for friends in different partitions
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                if(uid1 < uid2) {
                    Integer pid1 = uMap.get(uid1);
                    Integer pid2 = uMap.get(uid2);
                    if(!pid1.equals(pid2)) {
                        replicas.get(pid1).add(uid2);
                        replicas.get(pid2).add(uid1);
                        replicaLocations.get(uid1).add(pid2);
                        replicaLocations.get(uid2).add(pid1);
                    }
                }
            }
        }

        //Step 2: add replicas as necessary for k-replication
        for(Integer uid : replicaLocations.keySet()) {
            int numShort = minNumReplicas - replicaLocations.get(uid).size();
            if(numShort > 0) {
                Set<Integer> possibilities = new HashSet<>(partitions.keySet());
                possibilities.removeAll(replicaLocations.get(uid));
                possibilities.remove(uMap.get(uid));
                Set<Integer> newReplicas = getKDistinctValuesFromList(numShort, possibilities);
                for(Integer pid : newReplicas) {
                    replicas.get(pid).add(uid);
                }
            }
        }

        return replicas;
    }

    public static Map<Integer, Set<Integer>> copyMapSet(Map<Integer, Set<Integer>> m) {
        Map<Integer, Set<Integer>> copy = new HashMap<>();
        for(Integer key : m.keySet()) {
            copy.put(key, new HashSet<>(m.get(key)));
        }
        return copy;
    }


    //Taken from Java 8's java.util.Collections
    private static Random r;

    public static void shuffle(int arr[]) {
        Random rnd = r;
        if (rnd == null) {
            r = rnd = new Random();
        }

        for (int i=arr.length; i>1; i--) {
            int nextInt = rnd.nextInt(i);
            int tmp = arr[i-1];
            arr[i-1] = arr[nextInt];
            arr[nextInt] = tmp;
        }
    }

}
