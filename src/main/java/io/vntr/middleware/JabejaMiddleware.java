package io.vntr.middleware;

import io.vntr.User;
import io.vntr.manager.JManager;
import io.vntr.repartition.JRepartitioner;
import io.vntr.repartition.Results;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class JabejaMiddleware implements IMiddlewareAnalyzer{
    private float alpha;
    private float initialT;
    private float deltaT;
    private int k;
    private int numRestarts;
    private JManager manager;

    public JabejaMiddleware(float alpha, float initialT, float deltaT, int k, int numRestarts, JManager manager) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.numRestarts = numRestarts;
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
        Set<Integer> partition = manager.getPartition(partitionId);
        manager.removePartition(partitionId);
        for(Integer uid : partition) {
            Integer newPid = ProbabilityUtils.getRandomElement(manager.getPids());
            manager.moveUser(uid, newPid, true);
        }
    }

    @Override
    public Integer getNumberOfPartitions() {
        return manager.getNumPartitions();
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
        return 0; //Ja-be-Ja does not replicate
    }

    @Override
    public void broadcastDowntime() {
        repartition();
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

    public void repartition() {
        Results results = JRepartitioner.repartition(alpha, initialT, deltaT, k, numRestarts, getPartitionToUserMap(), getFriendships(), false);
        manager.increaseTallyLogical(results.getLogicalMoves());
        if(results.getUidsToPids() != null) {
            physicallyMigrate(results.getUidsToPids());
        }
    }

    void physicallyMigrate(Map<Integer, Integer> logicalPids) {
        for(Integer uid : logicalPids.keySet()) {
            User user = manager.getUser(uid);
            Integer newPid = logicalPids.get(uid);
            if(!user.getBasePid().equals(newPid)) {
                manager.moveUser(uid, newPid, false);
            }
        }
    }
}
