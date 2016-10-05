package io.vntr.hermes;

import io.vntr.IMiddleware;
import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesMiddleware  implements IMiddleware, IMiddlewareAnalyzer {
    private HermesManager manager;
    private HermesMigrator migrator;

    public HermesMiddleware(HermesManager manager, float gamma) {
        this.manager = manager;
        migrator = new HermesMigrator(manager, gamma);
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
        manager.repartition();
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
    public void addPartition() {
        manager.addPartition();
    }

    @Override
    public void removePartition(Integer partitionId) {
        migrator.migrateOffPartition(partitionId);
        manager.removePartition(partitionId);
    }

    @Override
    public Integer getNumberOfPartitions() {
        return manager.getAllPartitionIds().size();
    }

    @Override
    public Integer getNumberOfUsers() {
        return manager.getNumUsers();
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        return manager.getPartitionToUserMap();
    }

    @Override
    public Integer getReplicationCount() {
        return 0; //Hermes does not replicate
    }

    @Override
    public void broadcastDowntime() {
        manager.repartition();
    }
}
