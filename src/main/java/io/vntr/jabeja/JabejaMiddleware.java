package io.vntr.jabeja;

import io.vntr.IMiddleware;
import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class JabejaMiddleware implements IMiddleware, IMiddlewareAnalyzer {

    private JabejaManager manager;

    public JabejaMiddleware(JabejaManager manager) {
        this.manager = manager;
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(Long userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Long smallerUserId, Long largerUserId) {
        manager.befriend(smallerUserId, largerUserId);
        if(Math.random() > .9) {
            manager.repartition();
        }
    }

    @Override
    public void unfriend(Long smallerUserId, Long largerUserId) {
        manager.unfriend(smallerUserId, largerUserId);
    }

    @Override
    public void addPartition() {
        manager.addPartition();
    }

    @Override
    public void removePartition(Long partitionId) {
        Set<Long> partition = manager.getPartition(partitionId);
        manager.removePartition(partitionId);
        List<Long> pids = new LinkedList<Long>(manager.getPartitionIds());
        for(Long uid : partition) {
            Long newPid = ProbabilityUtils.getKDistinctValuesFromList(1, pids).iterator().next();
            manager.moveUser(uid, newPid);
        }
    }

    @Override
    public Long getNumberOfPartitions() {
        return manager.getNumPartitions();
    }

    @Override
    public Long getNumberOfUsers() {
        return manager.getNumUsers();
    }

    @Override
    public Long getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Map<Long, Set<Long>> getPartitionToUserMap() {
        return manager.getPartitionToUsers();
    }

    @Override
    public Long getReplicationCount() {
        return 0L; //Ja-be-Ja does not replicate
    }
}
