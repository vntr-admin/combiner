package io.vntr.jabeja;

import io.vntr.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class JabejaUser extends User {

    private float alpha;
    private Integer pid;
    private JabejaManager manager;

    public JabejaUser(Integer id, Integer initialPid, float alpha, JabejaManager manager) {
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

    public JabejaUser findPartner(Collection<JabejaUser> users, float t) {
        JabejaUser bestPartner = null;
        float bestScore = 0f;

        for(JabejaUser partner : users) {
            Integer theirPid = partner.getPid();
            if(theirPid == pid) {
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
