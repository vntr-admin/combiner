package io.vntr.spaja;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/28/16.
 * Should be done
 */
public class SpajaPartition {
    private Map<Integer, SpajaUser> idToMasterMap = new HashMap<>();
    private Map<Integer, SpajaUser> idToReplicaMap = new HashMap<>();
    private Integer id;

    public SpajaPartition(Integer id) {
        this.id = id;
    }

    public void addMaster(SpajaUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Integer id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(SpajaUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Integer id) {
        return idToReplicaMap.remove(id);
    }

    public SpajaUser getMasterById(Integer userId) {
        return idToMasterMap.get(userId);
    }

    public SpajaUser getReplicaById(Integer userId) {
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
