package io.vntr;

import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

public class User {
    private final short id;
    private short basePid = -1;
    private TShortSet friendIDs;

    public User(short id) {
        this.id = id;
        this.friendIDs = new TShortHashSet();
    }

    public User(short id, short basePid) {
        this.id = id;
        this.basePid = basePid;
        this.friendIDs = new TShortHashSet();
    }

    public short getId() {
        return id;
    }

    public short getBasePid() {
        return basePid;
    }

    public void setBasePid(short basePid) {
        this.basePid = basePid;
    }

    public void befriend(short friendId) {
        friendIDs.add(friendId);
    }

    public void unfriend(short friendId) {
        friendIDs.remove(friendId);
    }

    public TShortSet getFriendIDs() {
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
        if (basePid != user.basePid) return false;
        return friendIDs.equals(user.friendIDs);
    }

    @Override
    public int hashCode() {
        int result = (int) id;
        result = 31 * result + (int) basePid;
        result = 31 * result + friendIDs.hashCode();
        return result;
    }
}