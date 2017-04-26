package io.vntr.middleware;

import io.vntr.User;

public interface IMiddleware {

    int addUser();

    void addUser(User user);

    void removeUser(Integer userId);

    void befriend(Integer smallerUserId, Integer largerUserId);

    void unfriend(Integer smallerUserId, Integer largerUserId);

    int addPartition();

    void addPartition(Integer partitionId);

    void removePartition(Integer partitionId);

    void broadcastDowntime();
}