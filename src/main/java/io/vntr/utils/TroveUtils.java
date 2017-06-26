package io.vntr.utils;

import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;
import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import java.util.*;

/**
 * Created by robertlindquist on 6/22/17.
 */
public class TroveUtils {
    private static RandomEngine randomEngine = new DRand(((int) System.nanoTime()) >>> 2);

    public static boolean disjoint(TShortSet set1, TShortSet set2) {
        for(TShortIterator iter = set1.iterator(); iter.hasNext(); ) {
            if(set2.contains(iter.next())) {
                return false;
            }
        }
        return true;
    }

    public static Set<Short> convert(TShortSet trove) {
        if(trove == null) {
            return null;
        }
        Set<Short> set = new HashSet<>(trove.size()+1);
        for(TShortIterator iter = trove.iterator(); iter.hasNext(); ) {
            set.add(iter.next());
        }
        return set;
    }

    public static TShortShortMap convert1(Map<Short, Short> map) {
        if(map == null) {
            return null;
        }
        TShortShortMap tIntIntMap = new TShortShortHashMap(map.size()+1);
        for(short key : map.keySet()) {
            tIntIntMap.put(key, map.get(key));
        }
        return tIntIntMap;
    }

    public static Map<Short, Short> convert(TShortShortMap trove) {
        if(trove == null) {
            return null;
        }
        Map<Short, Short> map = new HashMap<>(trove.size()+1);
        for(short key : trove.keys()) {
            map.put(key, trove.get(key));
        }
        return map;
    }

    public static TShortObjectMap<TShortSet> convert(Map<Short, Set<Short>> mapSet) {
        if(mapSet== null) {
            return null;
        }
        TShortObjectMap<TShortSet> trove = new TShortObjectHashMap<>(mapSet.size() + 1);
        for(short pid : mapSet.keySet()) {
            trove.put(pid, new TShortHashSet(mapSet.get(pid)));
        }
        return trove;
    }

    public static Map<Short, Set<Short>> convert(TShortObjectMap<TShortSet> trove) {
        if(trove == null) {
            return null;
        }
        Map<Short, Set<Short>> mapSet = new HashMap<>();
        for(short pid : trove.keys()) {
            mapSet.put(pid, new HashSet<>(convert(trove.get(pid))));
        }
        return mapSet;
    }


    public static Short max(TShortSet tShortSet) {
        Short max = null;
        for(TShortIterator iter = tShortSet.iterator(); iter.hasNext(); ) {
            short next = iter.next();
            if(max == null || next > max) {
                max = next;
            }
        }
        return max;
    }

    public static Short min(TShortSet tIntSet) {
        Short min = null;
        for(TShortIterator iter = tIntSet.iterator(); iter.hasNext(); ) {
            short next = iter.next();
            if(min == null || next < min) {
                min = next;
            }
        }
        return min;
    }

    public static TShortShortMap getUserCounts(TShortObjectMap<TShortSet> partitions) {
        TShortShortMap map = new TShortShortHashMap(partitions.size()+1);
        for(short pid : partitions.keys()) {
            map.put(pid, (short) partitions.get(pid).size());
        }
        return map;
    }

    public static TShortShortMap getUToMasterMap(TShortObjectMap<TShortSet> partitions) {
        TShortShortMap map = new TShortShortHashMap(partitions.size() * 100);
        for(short pid : partitions.keys()) {
            for(TShortIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
                short uid = iter.next();
                map.put(uid, pid);
            }
        }
        return map;
    }

    public static TShortObjectMap<TShortSet> getUToReplicasMap(TShortObjectMap<TShortSet> replicaPartitions, TShortSet allUids) {
        TShortObjectMap<TShortSet> map = new TShortObjectHashMap<>(allUids.size()+1);
        for(TShortIterator iter = allUids.iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            map.put(uid, new TShortHashSet());
        }
        for(short pid : replicaPartitions.keys()) {
            for(TShortIterator iter = replicaPartitions.get(pid).iterator(); iter.hasNext(); ) {
                short uid = iter.next();
                map.get(uid).add(pid);
            }
        }
        return map;
    }

    public static Short getRandomElement(TShortSet tShortSet) {
        short size;
        if(tShortSet != null && (size = ((short)tShortSet.size())) > 0) {
            short index = (short)(size * Math.random());
            return tShortSet.toArray()[index];
        }

        return null;
    }

    public static TShortSet initSet(int... args) {
        TShortSet trove = new TShortHashSet(args.length+1);
        for(int i : args) {
            trove.add((short) i);
        }
        return trove;
    }

    public static TShortSet initSet(TShortSet initialSet, int... args) {
        TShortSet trove = new TShortHashSet(initialSet);
        for(int i : args) {
            trove.add((short) i);
        }
        return trove;
    }


    public static TShortObjectMap<TShortSet> generateBidirectionalFriendshipSet(TShortObjectMap<TShortSet> friendships) {
        TShortObjectMap<TShortSet> bidirectionalFriendshipSet = new TShortObjectHashMap<>(friendships.size()+1);
        for(short uid : friendships.keys()) {
            bidirectionalFriendshipSet.put(uid, new TShortHashSet());
        }
        for(short uid1 : friendships.keys()) {
            for(TShortIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                short uid2 = iter.next();
                bidirectionalFriendshipSet.get(uid1).add(uid2);
                bidirectionalFriendshipSet.get(uid2).add(uid1);
            }
        }
        return bidirectionalFriendshipSet;
    }

    public static TShortShortMap getPToFriendCount(short uid, TShortObjectMap<TShortSet> friendships, TShortShortMap uidToPidMap, TShortSet pids) {
        TShortShortMap pToFriendCount = new TShortShortHashMap(pids.size()+1);
        for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
            pToFriendCount.put(iter.next(), (short) 0);
        }
        for(TShortIterator iter = friendships.get(uid).iterator(); iter.hasNext(); ) {
            short pid = uidToPidMap.get(iter.next());
            pToFriendCount.put(pid, (short)(pToFriendCount.get(pid) + 1));
        }
        return pToFriendCount;
    }

    public static TShortSet getKDistinctValuesFromArray(short k, short[] array)
    {
        if(k >= array.length) {
            return new TShortHashSet(array);
        }

        long[] indices = new long[k];
        RandomSampler.sample(k, array.length, k, 0, indices, 0, randomEngine);

        TShortSet returnSet = new TShortHashSet(array.length+1);
        for(long index : indices) {
            returnSet.add(array[(int)index]);
        }
        return returnSet;
    }

    public static TShortSet intersection(TShortSet set1, TShortSet set2) {
        TShortSet intersection = new TShortHashSet();
        for(TShortIterator iter = set1.iterator(); iter.hasNext(); ) {
            short next = iter.next();
            if(set2.contains(next)) {
                intersection.add(next);
            }
        }
        return intersection;
    }

    public static TShortObjectMap<TShortSet> getInitialReplicasObeyingKReplication(short minNumReplicas, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships) {
        TShortObjectMap<TShortSet> replicas = new TShortObjectHashMap<>(partitions.size()+1);
        for(short pid : partitions.keys()) {
            replicas.put(pid, new TShortHashSet());
        }

        TShortObjectMap<TShortSet> replicaLocations = new TShortObjectHashMap<>(friendships.size()+1);
        for(short uid : friendships.keys()) {
            replicaLocations.put(uid, new TShortHashSet());
        }

        TShortShortMap uMap = getUToMasterMap(partitions);

        //Step 1: add replicas for friends in different partitions
        for(short uid1 : friendships.keys()) {
            for(TShortIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                short uid2 = iter.next();
                if(uid1 < uid2) {
                    short pid1 = uMap.get(uid1);
                    short pid2 = uMap.get(uid2);
                    if(pid1 != pid2) {
                        replicas.get(pid1).add(uid2);
                        replicas.get(pid2).add(uid1);
                        replicaLocations.get(uid1).add(pid2);
                        replicaLocations.get(uid2).add(pid1);
                    }
                }
            }
        }

        //Step 2: add replicas as necessary for k-replication
        for(short uid : replicaLocations.keys()) {
            short numShort = (short)(minNumReplicas - replicaLocations.get(uid).size());
            if(numShort > 0) {
                TShortSet possibilities = new TShortHashSet(partitions.keySet());
                possibilities.removeAll(replicaLocations.get(uid));
                possibilities.remove(uMap.get(uid));
                TShortSet newReplicas = getKDistinctValuesFromArray(numShort, possibilities.toArray());
                for(TShortIterator iter = newReplicas.iterator(); iter.hasNext(); ) {
                    replicas.get(iter.next()).add(uid);
                }
            }
        }

        return replicas;
    }

    public static TShortSet singleton(short val) {
        TShortSet set = new TShortHashSet(2);
        set.add(val);
        return set;
    }

    public static TShortObjectMap<TShortSet> copyTShortObjectMapIntSet(TShortObjectMap<TShortSet> map) {
        TShortObjectMap<TShortSet> retMap = new TShortObjectHashMap<>(map.size()+1);
        for(short key : map.keys()) {
            retMap.put(key, new TShortHashSet(map.get(key)));
        }
        return retMap;
    }

    public static short[] removeUniqueElementFromNonEmptyArray(short[] array, short uniqueElement) {
        short[] newArray = new short[array.length-1];
        boolean foundMasterPid = false;
        for(short i=0; i<array.length; i++) {
            short pid = array[i];
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

    public static void shuffle(short arr[]) {
        Random rnd = r;
        if (rnd == null) {
            r = rnd = new Random();
        }

        for (int i=arr.length; i>1; i--) {
            int nextInt = rnd.nextInt(i);
            short tmp = arr[i-1];
            arr[i-1] = arr[nextInt];
            arr[nextInt] = tmp;
        }
    }
}

