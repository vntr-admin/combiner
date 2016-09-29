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
    private Map<Long, SpajaUser> idToMasterMap = new HashMap<Long, SpajaUser>();
    private Map<Long, SpajaUser> idToReplicaMap = new HashMap<Long, SpajaUser>();
    private Long id;

    public SpajaPartition(Long id) {
        this.id = id;
    }

    public void addMaster(SpajaUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Long id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(SpajaUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Long id) {
        return idToReplicaMap.remove(id);
    }

    public SpajaUser getMasterById(Long userId) {
        return idToMasterMap.get(userId);
    }

    public SpajaUser getReplicaById(Long userId) {
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
