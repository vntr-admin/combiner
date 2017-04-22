package io.vntr.spj2;

import io.vntr.RepUser;
import io.vntr.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SpJ2Partition {
    private Map<Integer, RepUser> idToMasterMap = new HashMap<>();
    private Map<Integer, RepUser> idToReplicaMap = new HashMap<>();
    private Integer id;

    public SpJ2Partition(Integer id) {
        this.id = id;
    }

    public void addMaster(RepUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Integer id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(RepUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Integer id) {
        return idToReplicaMap.remove(id);
    }

    public RepUser getMasterById(Integer userId) {
        return idToMasterMap.get(userId);
    }

    public RepUser getReplicaById(Integer userId) {
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