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

    public HermesMiddleware(HermesManager manager, double gamma) {
        this.manager = manager;
        migrator = new HermesMigrator(manager, gamma);
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
        manager.repartition();
    }

    @Override
    public void removeUser(Long userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Long smallerUserId, Long largerUserId) {
        manager.befriend(smallerUserId, largerUserId);
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
        migrator.migrateOffPartition(partitionId);
        manager.removePartition(partitionId);
    }

    @Override
    public Long getNumberOfPartitions() {
        return (long) manager.getAllPartitionIds().size();
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
        return manager.getPartitionToUserMap();
    }
}
