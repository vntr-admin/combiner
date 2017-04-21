package io.vntr;

import java.util.HashSet;
import java.util.Set;

public class User {
    private Integer id;
    private Integer basePid;
    private Set<Integer> friendIDs;

    public User(Integer id) {
        this.id = id;
        this.friendIDs = new HashSet<>();
    }

    public User(Integer id, Integer basePid) {
        this.id = id;
        this.basePid = basePid;
        this.friendIDs = new HashSet<>();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getBasePid() {
        return basePid;
    }

    public void setBasePid(Integer basePid) {
        this.basePid = basePid;
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
        return id + "|F:" + friendIDs.toString() + "|P:" + basePid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof User)) return false;

        User user = (User) o;

        if (id != null ? !id.equals(user.id) : user.id != null) return false;
        if (basePid != null ? !basePid.equals(user.basePid) : user.basePid != null) return false;
        return friendIDs != null ? friendIDs.equals(user.friendIDs) : user.friendIDs == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (basePid != null ? basePid.hashCode() : 0);
        result = 31 * result + (friendIDs != null ? friendIDs.hashCode() : 0);
        return result;
    }
}