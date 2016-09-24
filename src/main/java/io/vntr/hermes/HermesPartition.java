package io.vntr.hermes;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesPartition {
    private Long id;
    private double gamma;
    private HermesManager manager;
    private Map<Long, HermesUser> physicalUsers;
    private Map<Long, LogicalUser> logicalUsers;

    public HermesPartition(Long id, double gamma, HermesManager manager) {
        this.id = id;
        this.gamma = gamma;
        this.manager = manager;
        this.physicalUsers = new HashMap<Long, HermesUser>();
        this.logicalUsers = new HashMap<Long, LogicalUser>();
    }

    public Long getId() {
        return id;
    }

    public HermesUser getUserById(Long uid) {
        return physicalUsers.get(uid);
    }

    public int getNumLogicalUsers() {
        return logicalUsers.size();
    }

    public void addLogicalUser(LogicalUser logicalUser) {
        this.logicalUsers.put(logicalUser.getId(), logicalUser);
    }

    public void removeLogicalUser(Long logicalUserId) {
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

    public Set<Long> physicallyMigrateCopy() {
        Set<Long> logicalUserSet = new HashSet<Long>(logicalUsers.keySet());
        logicalUserSet.removeAll(physicalUsers.keySet());
        for(Long newUserId : logicalUserSet) {
            HermesPartition p = manager.getPartitionById(manager.getPartitionIdForUser(newUserId));
            HermesUser newUser = p.getUserById(newUserId);
            physicalUsers.put(newUserId, newUser);
        }
        return logicalUserSet;
    }

    public void physicallyMigrateDelete() {
        for(Iterator<Long> iter = physicalUsers.keySet().iterator(); iter.hasNext(); ) {
            HermesUser user = physicalUsers.get(iter.next());
            Long logicalPid = user.getLogicalPid();
            if(!logicalPid.equals(id)) {
                user.setPhysicalPid(logicalPid);
                iter.remove();
            }
        }
    }

    public void resetLogicalUsers() {
        logicalUsers = new HashMap<Long, LogicalUser>();
        for(HermesUser hermesUser : physicalUsers.values()) {
            logicalUsers.put(hermesUser.getId(), hermesUser.getLogicalUser(true));
        }
    }

    public void updateLogicalUsersPartitionWeights(Map<Long, Long> partitionWeights) {
        for(LogicalUser logicalUser : logicalUsers.values()) {
            logicalUser.setpToWeight(partitionWeights);
        }
    }

    public void updateLogicalUsersTotalWeights(Long totalWeight) {
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

    public void removeUser(Long userId) {
        physicalUsers.remove(userId);
        logicalUsers.remove(userId);
    }

    public Set<Long> getPhysicalUserIds() {
        return Collections.unmodifiableSet(physicalUsers.keySet());
    }

    Set<Long> getLogicalUserIds() {
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
}
