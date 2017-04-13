package io.vntr.j2;

import io.vntr.User;

import java.util.Collection;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2User extends User{
    private float alpha;
    private Integer pid;
    private Integer logicalPid;
    private Integer bestLogicalPid;
    private J2Manager manager;

    public J2User(Integer id, Integer initialPid, float alpha, J2Manager manager) {
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

    public J2User findPartner(Collection<J2User> users, float t) {
        J2User bestPartner = null;
        float bestScore = 0f;

        for(J2User partner : users) {
            Integer theirLogicalPid = partner.getLogicalPid();
            if(theirLogicalPid.equals(logicalPid)) {
                continue;
            }

            int myNeighborsOnMine      = this.howManyFriendsHaveLogicalPartition(logicalPid);
            int myNeighborsOnTheirs    = this.howManyFriendsHaveLogicalPartition(theirLogicalPid);
            int theirNeighborsOnMine   = partner.howManyFriendsHaveLogicalPartition(logicalPid);
            int theirNeighborsOnTheirs = partner.howManyFriendsHaveLogicalPartition(theirLogicalPid);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, alpha) + Math.pow(theirNeighborsOnTheirs, alpha));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, alpha) + Math.pow(theirNeighborsOnMine, alpha));

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partner;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    int howManyFriendsHaveLogicalPartition(Integer logicalPid) {
        int count = 0;
        for(Integer friendId : getFriendIDs()) {
            Integer friendLogicalPid = manager.getUser(friendId).getLogicalPid();
            count += friendLogicalPid.equals(logicalPid) ? 1 : 0;
        }
        return count;
    }
}
