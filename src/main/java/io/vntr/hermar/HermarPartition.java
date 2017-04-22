package io.vntr.hermar;

import io.vntr.RepUser;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarPartition {
    private Integer id;
    private Map<Integer, RepUser> physicalUsers;

    public HermarPartition(Integer id) {
        this.id = id;
        this.physicalUsers = new HashMap<>();
    }

    public Integer getId() {
        return id;
    }

    public RepUser getUserById(Integer uid) {
        return physicalUsers.get(uid);
    }

    public int getNumUsers() {
        return physicalUsers.size();
    }

    public void addUser(RepUser RepUser) {
        physicalUsers.put(RepUser.getId(), RepUser);
    }

    public void removeUser(Integer userId) {
        physicalUsers.remove(userId);
    }

    public Set<Integer> getPhysicalUserIds() {
        return Collections.unmodifiableSet(physicalUsers.keySet());
    }

    @Override
    public String toString() {
        return "HermarPartition{" +
                "id=" + id +
                ", physicalUsers=" + physicalUsers +
                '}';
    }

}
