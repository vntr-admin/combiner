package io.vntr.replicadummy;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyPartition {
    private Map<Integer, ReplicaDummyUser> idToMasterMap = new HashMap<>();
    private Map<Integer, ReplicaDummyUser> idToReplicaMap = new HashMap<>();
    private Integer id;

    public ReplicaDummyPartition(Integer id) {
        this.id = id;
    }

    public void addMaster(ReplicaDummyUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Integer id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(ReplicaDummyUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Integer id) {
        return idToReplicaMap.remove(id);
    }

    public ReplicaDummyUser getMasterById(Integer userId) {
        return idToMasterMap.get(userId);
    }

    public ReplicaDummyUser getReplicaById(Integer userId) {
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
