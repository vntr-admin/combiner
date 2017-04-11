package io.vntr.jabar;

import io.vntr.User;

import java.util.Collection;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class JabarUser extends User {

    private float alpha;
    private Integer pid;
    private JabarManager manager;

    public JabarUser(Integer id, Integer initialPid, float alpha, JabarManager manager) {
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

    public JabarUser findPartner(Collection<JabarUser> users, float t) {
        JabarUser bestPartner = null;
        float bestScore = 0f;

        for(JabarUser partner : users) {
            Integer theirPid = partner.getPid();
            if(pid.equals(theirPid)) {
                continue;
            }

            int myNeighborsOnMine      = this.getNeighborsOnPartition(pid);
            int myNeighborsOnTheirs    = this.getNeighborsOnPartition(theirPid);
            int theirNeighborsOnMine   = partner.getNeighborsOnPartition(pid);
            int theirNeighborsOnTheirs = partner.getNeighborsOnPartition(theirPid);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, alpha) + Math.pow(theirNeighborsOnTheirs, alpha));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, alpha) + Math.pow(theirNeighborsOnMine, alpha));

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partner;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    public int getNeighborsOnPartition(Integer pid) {
        int count = 0;
        for(Integer friendId : getFriendIDs()) {
            count += manager.getPartitionForUser(friendId).equals(pid) ? 1 : 0;
        }

        return count;
    }

    public String toString() {
        return super.toString() + "|P:" + pid + "|alpha:" + alpha;
    }
}
