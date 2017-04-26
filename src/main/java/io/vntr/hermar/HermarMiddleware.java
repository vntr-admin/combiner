package io.vntr.hermar;

import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.HBefriender;
import io.vntr.hermes.HManager;
import io.vntr.migration.HMigrator;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarMiddleware implements IMiddlewareAnalyzer {
    private HManager manager;

    public HermarMiddleware(HManager manager) {
        this.manager = manager;
    }

    @Override
    public int addUser() {
        return manager.addUser();
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(Integer userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        manager.befriend(smallerUserId, largerUserId);
        handleRebalancing(smallerUserId, largerUserId);
    }

    void handleRebalancing(Integer smallerUserId, Integer largerUserId) {
        User smallerUser = manager.getUser(smallerUserId);
        User largerUser  = manager.getUser(largerUserId);

        BEFRIEND_REBALANCE_STRATEGY strategy = HBefriender.determineBestBefriendingRebalanceStrategy(smallerUser.getId(), largerUser.getId(), getGamma(), getFriendships(), getPartitionToUserMap());

        if(strategy == SMALL_TO_LARGE) {
            manager.moveUser(smallerUserId, largerUser.getBasePid(), false);
        }
        else if(strategy == LARGE_TO_SMALL) {
            manager.moveUser(largerUserId, smallerUser.getBasePid(), false);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        manager.unfriend(smallerUserId, largerUserId);
    }

    @Override
    public int addPartition() {
        return manager.addPartition();
    }

    @Override
    public void addPartition(Integer partitionId) {
        manager.addPartition(partitionId);
    }

    @Override
    public void removePartition(Integer partitionId) {
        Map<Integer, Integer> targets = HMigrator.migrateOffPartition(partitionId, manager.getGamma(), manager.getPartitionToUsers(), manager.getFriendships());
        for(Integer uid : targets.keySet()) {
            manager.moveUser(uid, targets.get(uid), true);
        }
        manager.removePartition(partitionId);
        manager.repartition();
    }

    @Override
    public Integer getNumberOfPartitions() {
        return manager.getPids().size();
    }

    @Override
    public Integer getNumberOfUsers() {
        return manager.getNumUsers();
    }

    @Override
    public Integer getNumberOfFriendships() {
        int numFriendships=0;
        Map<Integer, Set<Integer>> friendships = getFriendships();
        for(Integer uid : friendships.keySet()) {
            numFriendships += friendships.get(uid).size();
        }
        return numFriendships / 2;
    }

    @Override
    public Collection<Integer> getUserIds() {
        return manager.getUids();
    }

    @Override
    public Collection<Integer> getPartitionIds() {
        return manager.getPids();
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Long getMigrationTally() {
        return manager.getMigrationTally();
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        return manager.getPartitionToUsers();
    }

    @Override
    public Integer getReplicationCount() {
        return 0; //Hermar does not replicate
    }

    @Override
    public void broadcastDowntime() {
        manager.repartition();
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        return manager.getFriendships();
    }

    @Override
    public double calculateAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicaMap() {
        Map<Integer, Set<Integer>> m = new HashMap<>();
        for(int pid : getPartitionIds()) {
            m.put(pid, new HashSet<Integer>());
        }
        return m;
    }

    public float getGamma() {
        return manager.getGamma();
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    @Override
    public double calculateExpectedQueryDelay() {
        return ProbabilityUtils.calculateExpectedQueryDelay(getFriendships(), getPartitionToUserMap());
    }

    @Override
    public void checkValidity() {
        manager.checkValidity();
    }
}
