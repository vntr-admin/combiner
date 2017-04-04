package io.vntr.spar;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SparPartition {
    private Map<Integer, SparUser> idToMasterMap = new HashMap<>();
    private Map<Integer, SparUser> idToReplicaMap = new HashMap<>();
    private Integer id;

    public SparPartition(Integer id) {
        this.id = id;
    }

    public void addMaster(SparUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Integer id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(SparUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Integer id) {
        return idToReplicaMap.remove(id);
    }

    public SparUser getMasterById(Integer userId) {
        return idToMasterMap.get(userId);
    }

    public SparUser getReplicaById(Integer userId) {
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