package io.vntr;

import java.util.HashSet;
import java.util.Set;

public class User {
    private Integer id;
    //TODO: you may as well move pid or masterPid in here
    private Set<Integer> friendIDs;

    public User(Integer id) {
        this.id = id;
        this.friendIDs = new HashSet<>();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void befriend(Integer friendId) {
        friendIDs.add(friendId);
    }

    public void unfriend(Integer friendId) {
        friendIDs.remove(friendId);
    }

    public Set<Integer> getFriendIDs() {
        return friendIDs;
    }

    @Override
    public String toString() {
        return id + "|F:" + friendIDs.toString();
    }
}