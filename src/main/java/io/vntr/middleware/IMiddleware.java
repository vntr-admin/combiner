package io.vntr.middleware;

import io.vntr.User;

public interface IMiddleware {

    int addUser();

    void addUser(User user);

    void removeUser(Integer uid);

    void befriend(Integer smallerUid, Integer largerUid);

    void unfriend(Integer smallerUid, Integer largerUid);

    int addPartition();

    void addPartition(Integer pid);

    void removePartition(Integer pid);

    void broadcastDowntime();
}