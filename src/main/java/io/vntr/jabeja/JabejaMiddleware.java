package io.vntr.jabeja;

import io.vntr.IMiddleware;
import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class JabejaMiddleware implements IMiddlewareAnalyzer {

    private JabejaManager manager;

    public JabejaMiddleware(JabejaManager manager) {
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
        if(Math.random() > .9) {
            manager.repartition();
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
        Set<Integer> partition = manager.getPartition(partitionId);
        manager.removePartition(partitionId);
        for(Integer uid : partition) {
            Integer newPid = ProbabilityUtils.getRandomElement(manager.getPartitionIds());
            manager.moveUser(uid, newPid);
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
    public Collection<Integer> getUserIds() {
        return manager.getUserIds();
    }

    @Override
    public Collection<Integer> getPartitionIds() {
        return manager.getAllPartitionIds();
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
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
        manager.repartition();
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        return manager.getFriendships();
    }

    @Override
    public double calcualteAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicaMap() {
        Map<Integer, Set<Integer>> m = new HashMap<Integer, Set<Integer>>();
        for(int pid : getPartitionIds()) {
            m.put(pid, new HashSet<Integer>());
        }
        return m;
    }
}
