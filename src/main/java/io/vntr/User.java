package io.vntr;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class User {
    private final int id;
    private Integer basePid;
    private TIntSet friendIDs;

    public User(Integer id) {
        this.id = id;
        this.friendIDs = new TIntHashSet();
    }

    public User(int id, Integer basePid) {
        this.id = id;
        this.basePid = basePid;
        this.friendIDs = new TIntHashSet();
    }

    public int getId() {
        return id;
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

    public TIntSet getFriendIDs() {
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

        if (id != user.id) return false;
        if (basePid != null ? !basePid.equals(user.basePid) : user.basePid != null) return false;
        return friendIDs != null ? friendIDs.equals(user.friendIDs) : user.friendIDs == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (basePid != null ? basePid.hashCode() : 0);
        result = 31 * result + (friendIDs != null ? friendIDs.hashCode() : 0);
        return result;
    }
}