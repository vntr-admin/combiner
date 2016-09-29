package io.vntr.sparmes;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesPartition {
    private Map<Long, SparmesUser> idToMasterMap = new HashMap<Long, SparmesUser>();
    private Map<Long, SparmesUser> idToReplicaMap = new HashMap<Long, SparmesUser>();
    private Long id;

    public SparmesPartition(Long id) {
        this.id = id;
    }

    public void addMaster(SparmesUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Long id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(SparmesUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Long id) {
        return idToReplicaMap.remove(id);
    }

    public SparmesUser getMasterById(Long userId) {
        return idToMasterMap.get(userId);
    }

    public SparmesUser getReplicaById(Long userId) {
        return idToReplicaMap.get(userId);
    }

    public int getNumMasters() {
        return idToMasterMap.size();
    }

    public int getNumReplicas() {
        return idToReplicaMap.size();
    }

    public Set<Long> getIdsOfMasters() {
        return idToMasterMap.keySet();
    }

    public Set<Long> getIdsOfReplicas() {
        return idToReplicaMap.keySet();
    }

    public Long getId() {
        return id;
    }
}
