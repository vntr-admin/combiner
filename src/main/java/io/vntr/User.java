package io.vntr;

import java.util.HashSet;
import java.util.Set;

public class User {
    private String name;
    private Integer id;
    private Set<Integer> friendIDs;

    public User(String name, Integer id) {
        this.name = name;
        this.id = id;
        this.friendIDs = new HashSet<Integer>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        return id + "|" + name + "|friends:" + friendIDs.toString();
    }
}