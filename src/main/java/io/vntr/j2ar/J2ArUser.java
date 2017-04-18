package io.vntr.j2ar;

import io.vntr.User;

import java.util.Collection;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2ArUser extends User{
    private float alpha;
    private Integer pid;
    private Integer logicalPid;
    private Integer bestLogicalPid;
    private J2ArManager manager;

    public J2ArUser(Integer id, Integer initialPid, float alpha, J2ArManager manager) {
        super(id);
        this.pid = initialPid;
        this.alpha = alpha;
        this.manager = manager;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public Integer getLogicalPid() {
        return logicalPid;
    }

    public void setLogicalPid(Integer logicalPid) {
        this.logicalPid = logicalPid;
    }

    public Integer getBestLogicalPid() {
        return bestLogicalPid;
    }

    public void setBestLogicalPid(Integer bestLogicalPid) {
        this.bestLogicalPid = bestLogicalPid;
    }

    public J2ArUser findPartner(Collection<J2ArUser> users, float t, boolean useLogicalPids) {
        J2ArUser bestPartner = null;
        float bestScore = 0f;

        Integer myPid = useLogicalPids ? logicalPid : pid;

        for(J2ArUser partner : users) {
            Integer theirPid = useLogicalPids ? partner.getLogicalPid() : partner.getPid();
            if(theirPid.equals(myPid)) {
                continue;
            }

            int myNeighborsOnMine      = this.howManyFriendsHavePartition(myPid, useLogicalPids);
            int myNeighborsOnTheirs    = this.howManyFriendsHavePartition(theirPid, useLogicalPids);
            int theirNeighborsOnMine   = partner.howManyFriendsHavePartition(myPid, useLogicalPids);
            int theirNeighborsOnTheirs = partner.howManyFriendsHavePartition(theirPid, useLogicalPids);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, alpha) + Math.pow(theirNeighborsOnTheirs, alpha));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, alpha) + Math.pow(theirNeighborsOnMine, alpha));

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partner;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    int howManyFriendsHavePartition(Integer pid, boolean useLogicalPids) {
        int count = 0;
        for(Integer friendId : getFriendIDs()) {
            J2ArUser friend = manager.getUser(friendId);
            Integer friendPid = useLogicalPids ? friend.getLogicalPid() : friend.getPid();
            count += friendPid.equals(pid) ? 1 : 0;
        }
        return count;
    }

    public int getNeighborsOnPartition(Integer pid) {
        int count = 0;
        for(Integer friendId : getFriendIDs()) {
            count += manager.getUser(friendId).getPid().equals(pid) ? 1 : 0;
        }

        return count;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof J2ArUser)) return false;

        J2ArUser that = (J2ArUser) o;

        return  (Float.compare(that.alpha, alpha) == 0)
                && safeEquals(this.pid,            that.pid)
                && safeEquals(this.logicalPid,     that.logicalPid)
                && safeEquals(this.bestLogicalPid, that.bestLogicalPid)
                && safeEquals(this.getId(),        that.getId())
                && safeEquals(this.getFriendIDs(), that.getFriendIDs());
    }

    @Override
    public int hashCode() {
        int result = (alpha != +0.0f ? Float.floatToIntBits(alpha) : 0);
        result = 31 * result + safeHashCode(pid);
        result = 31 * result + safeHashCode(logicalPid);
        result = 31 * result + safeHashCode(bestLogicalPid);
        result = 31 * result + safeHashCode(getId());
        result = 31 * result + safeHashCode(getFriendIDs());
        return result;
    }
}
