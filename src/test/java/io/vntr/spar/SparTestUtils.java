package io.vntr.spar;

import io.vntr.RepUser;
import io.vntr.TestUtils;
import io.vntr.User;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.Utils.*;

public class SparTestUtils {
    public static final Integer[] STANDAR_USER_ID_ARRAY = {3, 4, 5, 6, 8, 10, 11, 12};

    public static SparManager getStandardManager() {
        SparManager manager = new SparManager(2);

        for (int i = 0; i < 5; i++) {
            manager.addPartition();
        }

        for (int i = 0; i < 8; i++) {
            manager.addUser(new User(STANDAR_USER_ID_ARRAY[i]));
        }

        return manager;
    }

    public static Set<Integer> getPartitionsWithAPresence(SparManager manager, Integer userId) {
        RepUser user = manager.getUserMasterById(userId);
        Set<Integer> partitionsWithAPresence = new HashSet<>(user.getReplicaPids());
        partitionsWithAPresence.add(user.getBasePid());
        return partitionsWithAPresence;
    }

    public static Set<Integer> getPartitionsWithNoPresence(SparManager manager, Integer userId) {
        Set<Integer> partitionsWithoutAPresence = new HashSet<>(manager.getPids());
        partitionsWithoutAPresence.removeAll(getPartitionsWithAPresence(manager, userId));
        return partitionsWithoutAPresence;
    }

    public static RepUser getUserWithMasterOnPartition(SparManager manager, Integer partitionId) {
        SparPartition partition = manager.getPartitionById(partitionId);
        Integer userId = partition.getIdsOfMasters().iterator().next();
        return manager.getUserMasterById(userId);
    }

    public static Set<Integer> getPartitionIdsWithNMasters(SparManager manager, int n) {
        Set<Integer> partitionIdsWithNMasters = new HashSet<>();
        for (Integer partitionId : manager.getPids()) {
            if (manager.getPartitionById(partitionId).getNumMasters() == n) {
                partitionIdsWithNMasters.add(partitionId);
            }
        }

        return partitionIdsWithNMasters;
    }

    public static SparManager initGraph(int minNumReplicas, int numPartitions, Map<Integer, Set<Integer>> friendships) {
        Set<Integer> pids = new HashSet<>();
        for(int pid = 0; pid < numPartitions; pid++) {
            pids.add(pid);
        }
        Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
        Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(minNumReplicas, partitions, friendships);
        return initGraph(minNumReplicas, partitions, friendships, replicas);
    }

    public static SparManager initGraph(int minNumReplicas, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicaPartitions) {
        SparManager manager = new SparManager(minNumReplicas);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
        }

        Map<Integer, Integer> uToMasterMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uToReplicasMap = getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Integer uid : friendships.keySet()) {
            RepUser user = new RepUser(uid);
            Integer pid = uToMasterMap.get(uid);
            user.setBasePid(pid);

            manager.addUser(user, pid);
        }

        for(Integer uid : friendships.keySet()) {
            for (Integer rPid : uToReplicasMap.get(uid)) {
                manager.addReplica(manager.getUserMasterById(uid), rPid);
            }
        }

        for(Integer uid : friendships.keySet()) {
            for(Integer friendId : friendships.get(uid)) {
                manager.befriend(manager.getUserMasterById(uid), manager.getUserMasterById(friendId));
            }
        }

        return manager;
    }

}