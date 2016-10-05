package io.vntr.hermes;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesPartition {
    private Integer id;
    private float gamma;
    private HermesManager manager;
    private Map<Integer, HermesUser> physicalUsers;
    private Map<Integer, LogicalUser> logicalUsers;

    public HermesPartition(Integer id, float gamma, HermesManager manager) {
        this.id = id;
        this.gamma = gamma;
        this.manager = manager;
        this.physicalUsers = new HashMap<Integer, HermesUser>();
        this.logicalUsers = new HashMap<Integer, LogicalUser>();
    }

    public Integer getId() {
        return id;
    }

    public HermesUser getUserById(Integer uid) {
        return physicalUsers.get(uid);
    }

    public int getNumLogicalUsers() {
        return logicalUsers.size();
    }

    public void addLogicalUser(LogicalUser logicalUser) {
        this.logicalUsers.put(logicalUser.getId(), logicalUser);
    }

    public void removeLogicalUser(Integer logicalUserId) {
        this.logicalUsers.remove(logicalUserId);
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

    /** Returns the users who were copied to this partition. */
    public Set<Integer> physicallyMigrateCopy() {
        Set<Integer> logicalUserSet = new HashSet<Integer>(logicalUsers.keySet());
        logicalUserSet.removeAll(physicalUsers.keySet());
        for(Integer newUserId : logicalUserSet) {
            physicalUsers.put(newUserId, manager.getUser(newUserId));
        }
        return logicalUserSet;
    }

    public void physicallyMigrateDelete() {
        for(Iterator<Integer> iter = physicalUsers.keySet().iterator(); iter.hasNext(); ) {
            HermesUser user = physicalUsers.get(iter.next());
            Integer logicalPid = user.getLogicalPid();
            if(!logicalPid.equals(id)) {
                user.setPhysicalPid(logicalPid);
                iter.remove();
            }
        }
    }

    public void resetLogicalUsers() {
        logicalUsers = new HashMap<Integer, LogicalUser>();
        for(HermesUser hermesUser : physicalUsers.values()) {
            logicalUsers.put(hermesUser.getId(), hermesUser.getLogicalUser(true));
        }
    }

    public void updateLogicalUsersPartitionWeights(Map<Integer, Integer> partitionWeights) {
        for(LogicalUser logicalUser : logicalUsers.values()) {
            logicalUser.setpToWeight(partitionWeights);
        }
    }

    public void updateLogicalUsersTotalWeights(Integer totalWeight) {
        for(LogicalUser logicalUser : logicalUsers.values()) {
            logicalUser.setTotalWeight(totalWeight);
        }
    }

    public int getNumUsers() {
        return physicalUsers.size();
    }

    public void addUser(HermesUser hermesUser) {
        physicalUsers.put(hermesUser.getId(), hermesUser);
    }

    public void removeUser(Integer userId) {
        physicalUsers.remove(userId);
        logicalUsers.remove(userId);
    }

    public Set<Integer> getPhysicalUserIds() {
        return Collections.unmodifiableSet(physicalUsers.keySet());
    }

    Set<Integer> getLogicalUserIds() {
        return Collections.unmodifiableSet(logicalUsers.keySet());
    }

    @Override
    public String toString() {
        return "HermesPartition{" +
                "id=" + id +
                ", gamma=" + gamma +
                ", physicalUsers=" + physicalUsers +
                ", logicalUsers=" + logicalUsers +
                '}';
    }

    public void updateLogicalUserFriendCounts(Integer logicalUid, Map<Integer, Integer> updatedFriendCounts) {
        logicalUsers.get(logicalUid).setPToFriendCount(updatedFriendCounts);
    }
}
