package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesPartition {
    private Map<Long, SparmesUser> idToMasterMap = new HashMap<Long, SparmesUser>();
    private Map<Long, SparmesUser> idToReplicaMap = new HashMap<Long, SparmesUser>();
    private Map<Long, LogicalUser> logicalUsers = new HashMap<Long, LogicalUser>();
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

    public void resetLogicalUsers() {
        logicalUsers = new HashMap<Long, LogicalUser>();
        for(SparmesUser user : idToMasterMap.values()) {
            logicalUsers.put(user.getId(), user.getLogicalUser(true));
        }
    }

    public Set<Target> getCandidates(boolean firstIteration, int k) {
        NavigableSet<Target> candidates = new TreeSet<Target>();
        for(LogicalUser luser : logicalUsers.values()) {
            Target target = luser.getTargetPart(firstIteration);
            if(target.partitionId != null) {
                candidates.add(target);
            }
        }

        Set<Target> topKCandidates = new HashSet<Target>();
        int i=0;
        for(Iterator<Target> iter = candidates.descendingIterator(); iter.hasNext() && i++<k; ) {
            topKCandidates.add(iter.next());
        }

        return topKCandidates;
    }


    public Set<Long> physicallyMigrateCopy() {
        //TODO: do this
        return Collections.emptySet();
    }

    public void physicallyMigrateDelete() {
        //TODO: do this
    }

    public int getNumLogicalUsers() {
        return logicalUsers.size();
    }
}
