package io.vntr.spj2;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SpJ2Partition {
    private Map<Integer, SpJ2User> idToMasterMap = new HashMap<>();
    private Map<Integer, SpJ2User> idToReplicaMap = new HashMap<>();
    private Integer id;

    public SpJ2Partition(Integer id) {
        this.id = id;
    }

    public void addMaster(SpJ2User user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Integer id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(SpJ2User user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Integer id) {
        return idToReplicaMap.remove(id);
    }

    public SpJ2User getMasterById(Integer userId) {
        return idToMasterMap.get(userId);
    }

    public SpJ2User getReplicaById(Integer userId) {
        return idToReplicaMap.get(userId);
    }

    public int getNumMasters() {
        return idToMasterMap.size();
    }

    public int getNumReplicas() {
        return idToReplicaMap.size();
    }

    public Set<Integer> getIdsOfMasters() {
        return idToMasterMap.keySet();
    }

    public Set<Integer> getIdsOfReplicas() {
        return idToReplicaMap.keySet();
    }

    public Integer getId() {
        return id;
    }
}