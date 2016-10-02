package io.vntr;

public interface IMiddleware {
    void addUser(User user);

    void removeUser(Long userId);

    void befriend(Long smallerUserId, Long largerUserId);

    void unfriend(Long smallerUserId, Long largerUserId);

    void addPartition();

    void removePartition(Long partitionId);

    void broadcastDowntime();
}