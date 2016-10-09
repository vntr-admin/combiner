package io.vntr;

public interface IMiddleware {

    int addUser();

    void addUser(User user);

    void removeUser(Integer userId);

    void befriend(Integer smallerUserId, Integer largerUserId);

    void unfriend(Integer smallerUserId, Integer largerUserId);

    int addPartition();

    void removePartition(Integer partitionId);

    void broadcastDowntime();
}