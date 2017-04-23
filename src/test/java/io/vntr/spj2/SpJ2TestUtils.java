package io.vntr.spj2;

import io.vntr.RepUser;
import io.vntr.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SpJ2TestUtils {
    public static final Integer[] STANDAR_USER_ID_ARRAY = {3, 4, 5, 6, 8, 10, 11, 12};

    public static SpJ2Manager getStandardManager() {
        SpJ2Manager manager = new SpJ2Manager(2, 1f, 2, 0.5f, 15, 0);

        for (int i = 0; i < 5; i++) {
            manager.addPartition();
        }

        for (int i = 0; i < 8; i++) {
            manager.addUser(new User(STANDAR_USER_ID_ARRAY[i]));
        }

        return manager;
    }

    public static Set<Integer> getPartitionsWithAPresence(SpJ2Manager manager, Integer userId) {
        RepUser user = manager.getUserMasterById(userId);
        Set<Integer> partitionsWithAPresence = new HashSet<>(user.getReplicaPids());
        partitionsWithAPresence.add(user.getBasePid());
        return partitionsWithAPresence;
    }

    public static Set<Integer> getPartitionsWithNoPresence(SpJ2Manager manager, Integer userId) {
        Set<Integer> partitionsWithoutAPresence = new HashSet<>(manager.getPids());
        partitionsWithoutAPresence.removeAll(getPartitionsWithAPresence(manager, userId));
        return partitionsWithoutAPresence;
    }

    public static RepUser getUserWithMasterOnPartition(SpJ2Manager manager, Integer partitionId) {
        SpJ2Partition partition = manager.getPartitionById(partitionId);
        Integer userId = partition.getIdsOfMasters().iterator().next();
        return manager.getUserMasterById(userId);
    }

    public static Set<Integer> getPartitionIdsWithNMasters(SpJ2Manager manager, int n) {
        Set<Integer> partitionIdsWithNMasters = new HashSet<>();
        for (Integer partitionId : manager.getPids()) {
            if (manager.getPartitionById(partitionId).getNumMasters() == n) {
                partitionIdsWithNMasters.add(partitionId);
            }
        }

        return partitionIdsWithNMasters;
    }

}