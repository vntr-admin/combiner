package io.vntr;

public interface IMiddleware {
    void addUser(User user);

    void removeUser(Integer userId);

    void befriend(Integer smallerUserId, Integer largerUserId);

    void unfriend(Integer smallerUserId, Integer largerUserId);

    void addPartition();

    void removePartition(Integer partitionId);

    void broadcastDowntime();
}