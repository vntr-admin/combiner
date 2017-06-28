package io.vntr.utils;

import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.*;

/**
 * Created by robertlindquist on 6/22/17.
 */
public class TroveUtils {
    private static RandomEngine randomEngine = new DRand(((int) System.nanoTime()) >>> 2);

    public static boolean disjoint(TIntSet set1, TIntSet set2) {
        for(TIntIterator iter = set1.iterator(); iter.hasNext(); ) {
            if(set2.contains(iter.next())) {
                return false;
            }
        }
        return true;
    }

    public static Set<Integer> convert(TIntSet trove) {
        if(trove == null) {
            return null;
        }
        Set<Integer> set = new HashSet<>(trove.size()+1);
        for(TIntIterator iter = trove.iterator(); iter.hasNext(); ) {
            set.add(iter.next());
        }
        return set;
    }

    public static TIntIntMap convert1(Map<Integer, Integer> map) {
        if(map == null) {
            return null;
        }
        TIntIntMap tIntIntMap = new TIntIntHashMap(map.size()+1);
        for(int key : map.keySet()) {
            tIntIntMap.put(key, map.get(key));
        }
        return tIntIntMap;
    }

    public static Map<Integer, Integer> convert(TIntIntMap trove) {
        if(trove == null) {
            return null;
        }
        Map<Integer, Integer> map = new HashMap<>(trove.size()+1);
        for(int key : trove.keys()) {
            map.put(key, trove.get(key));
        }
        return map;
    }

    public static TIntObjectMap<TIntSet> convert(Map<Integer, Set<Integer>> mapSet) {
        if(mapSet== null) {
            return null;
        }
        TIntObjectMap<TIntSet> trove = new TIntObjectHashMap<>(mapSet.size() + 1);
        for(int pid : mapSet.keySet()) {
            trove.put(pid, new TIntHashSet(mapSet.get(pid)));
        }
        return trove;
    }

    public static Map<Integer, Set<Integer>> convert(TIntObjectMap<TIntSet> trove) {
        if(trove == null) {
            return null;
        }
        Map<Integer, Set<Integer>> mapSet = new HashMap<>();
        for(int pid : trove.keys()) {
            mapSet.put(pid, new HashSet<>(convert(trove.get(pid))));
        }
        return mapSet;
    }


    public static Integer max(TIntSet tIntSet) {
        Integer max = null;
        for(TIntIterator iter = tIntSet.iterator(); iter.hasNext(); ) {
            int next = iter.next();
            if(max == null || next > max) {
                max = next;
            }
        }
        return max;
    }

    public static Integer min(TIntSet tIntSet) {
        Integer min = null;
        for(TIntIterator iter = tIntSet.iterator(); iter.hasNext(); ) {
            int next = iter.next();
            if(min == null || next < min) {
                min = next;
            }
        }
        return min;
    }

    public static TIntIntMap getUserCounts(TIntObjectMap<TIntSet> partitions) {
        TIntIntMap map = new TIntIntHashMap(partitions.size()+1);
        for(int pid : partitions.keys()) {
            map.put(pid, partitions.get(pid).size());
        }
        return map;
    }

    public static TIntIntMap getUToMasterMap(TIntObjectMap<TIntSet> partitions) {
        TIntIntMap map = new TIntIntHashMap(partitions.size() * 100);
        for(int pid : partitions.keys()) {
            for(TIntIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
                int uid = iter.next();
                map.put(uid, pid);
            }
        }
        return map;
    }

    public static TIntObjectMap<TIntSet> getUToReplicasMap(TIntObjectMap<TIntSet> replicaPartitions, TIntSet allUids) {
        TIntObjectMap<TIntSet> map = new TIntObjectHashMap<>(allUids.size()+1);
        for(TIntIterator iter = allUids.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            map.put(uid, new TIntHashSet());
        }
        for(int pid : replicaPartitions.keys()) {
            for(TIntIterator iter = replicaPartitions.get(pid).iterator(); iter.hasNext(); ) {
                int uid = iter.next();
                map.get(uid).add(pid);
            }
        }
        return map;
    }

    public static Integer getRandomElement(TIntSet tIntSet) {
        int size;
        if(tIntSet != null && (size = tIntSet.size()) > 0) {
            int index = (int)(size * Math.random());
            return tIntSet.toArray()[index];
        }

        return null;
    }

    public static TIntSet initSet(int... args) {
        TIntSet trove = new TIntHashSet(args.length+1);
        trove.addAll(args);
        return trove;
    }

    public static TIntSet initSet(TIntSet initialSet, int... args) {
        TIntSet trove = new TIntHashSet(initialSet);
        trove.addAll(args);
        return trove;
    }


    public static TIntObjectMap<TIntSet> generateBidirectionalFriendshipSet(TIntObjectMap<TIntSet> friendships) {
        TIntObjectMap<TIntSet> bidirectionalFriendshipSet = new TIntObjectHashMap<>(friendships.size()+1);
        for(int uid : friendships.keys()) {
            bidirectionalFriendshipSet.put(uid, new TIntHashSet());
        }
        for(int uid1 : friendships.keys()) {
            for(TIntIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                int uid2 = iter.next();
                bidirectionalFriendshipSet.get(uid1).add(uid2);
                bidirectionalFriendshipSet.get(uid2).add(uid1);
            }
        }
        return bidirectionalFriendshipSet;
    }

    public static TIntIntMap getPToFriendCount(Integer uid, TIntObjectMap<TIntSet> friendships, TIntIntMap uidToPidMap, TIntSet pids) {
        TIntIntMap pToFriendCount = new TIntIntHashMap(pids.size()+1);
        for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
            pToFriendCount.put(iter.next(), 0);
        }
        for(TIntIterator iter = friendships.get(uid).iterator(); iter.hasNext(); ) {
            Integer pid = uidToPidMap.get(iter.next());
            pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
        }
        return pToFriendCount;
    }

    public static TIntSet getKDistinctValuesFromArray(int k, int[] array)
    {
        if(k >= array.length) {
            return new TIntHashSet(array);
        }

        long[] indices = new long[k];
        RandomSampler.sample(k, array.length, k, 0, indices, 0, randomEngine);

        TIntSet returnSet = new TIntHashSet(array.length+1);
        for(long index : indices) {
            returnSet.add(array[(int)index]);
        }
        return returnSet;
    }

    public static TIntSet intersection(TIntSet set1, TIntSet set2) {
        TIntSet intersection = new TIntHashSet();
        for(TIntIterator iter = set1.iterator(); iter.hasNext(); ) {
            int next = iter.next();
            if(set2.contains(next)) {
                intersection.add(next);
            }
        }
        return intersection;
    }

    public static TIntObjectMap<TIntSet> getInitialReplicasObeyingKReplication(int minNumReplicas, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships) {
        TIntObjectMap<TIntSet> replicas = new TIntObjectHashMap<>(partitions.size()+1);
        for(Integer pid : partitions.keys()) {
            replicas.put(pid, new TIntHashSet());
        }

        TIntObjectMap<TIntSet> replicaLocations = new TIntObjectHashMap<>(friendships.size()+1);
        for(Integer uid : friendships.keys()) {
            replicaLocations.put(uid, new TIntHashSet());
        }

        TIntIntMap uMap = getUToMasterMap(partitions);

        //Step 1: add replicas for friends in different partitions
        for(Integer uid1 : friendships.keys()) {
            for(TIntIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                int uid2 = iter.next();
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
        for(Integer uid : replicaLocations.keys()) {
            int numShort = minNumReplicas - replicaLocations.get(uid).size();
            if(numShort > 0) {
                TIntSet possibilities = new TIntHashSet(partitions.keySet());
                possibilities.removeAll(replicaLocations.get(uid));
                possibilities.remove(uMap.get(uid));
                TIntSet newReplicas = getKDistinctValuesFromArray(numShort, possibilities.toArray());
                for(TIntIterator iter = newReplicas.iterator(); iter.hasNext(); ) {
                    replicas.get(iter.next()).add(uid);
                }
            }
        }

        return replicas;
    }

    public static TIntSet singleton(int val) {
        TIntSet set = new TIntHashSet(2);
        set.add(val);
        return set;
    }

    public static TIntObjectMap<TIntSet> copyTIntObjectMapIntSet(TIntObjectMap<TIntSet> map) {
        TIntObjectMap<TIntSet> retMap = new TIntObjectHashMap<>(map.size()+1);
        for(int key : map.keys()) {
            retMap.put(key, new TIntHashSet(map.get(key)));
        }
        return retMap;
    }

    public static int[] removeUniqueElementFromNonEmptyArray(int[] array, int uniqueElement) {
        int[] newArray = new int[array.length-1];
        boolean foundMasterPid = false;
        for(int i=0; i<array.length; i++) {
            int pid = array[i];
            if(pid == uniqueElement) {
                foundMasterPid = true;
            }
            else if(foundMasterPid) {
                newArray[i-1] = pid;
            }
            else {
                newArray[i] = pid;
            }
        }
        return newArray;
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

    public static TIntObjectMap<TIntSet> invertTIntIntMap(TIntIntMap original) {
        if(original == null) {
            return null;
        }
        TIntObjectMap<TIntSet> inverted = new TIntObjectHashMap<>();
        for(int key : original.keys()) {
            int value = original.get(key);
            if(!inverted.containsKey(value)) {
                inverted.put(value, new TIntHashSet());
            }
            inverted.get(value).add(key);
        }
        return inverted;
    }
}

