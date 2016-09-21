package io.vntr.jabeja;

import io.vntr.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class JabejaUser extends User {

    private double alpha;
    private Long pid;
    private JabejaManager manager;
    private Set<JabejaUser> friends;

    public JabejaUser(String name, Long id, Long initialPid, double alpha, JabejaManager manager) {
        super(name, id);
        this.pid = initialPid;
        this.alpha = alpha;
        this.manager = manager;
        friends = new HashSet<JabejaUser>();
    }

    @Override
    public void befriend(Long friendId) {
        super.befriend(friendId);
        friends.add(manager.getUser(friendId));
    }

    @Override
    public void unfriend(Long friendId) {
        super.unfriend(friendId);
        friends.remove(manager.getUser(friendId));
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public JabejaUser findPartner(Collection<JabejaUser> users, double t) {
        JabejaUser bestPartner = null;
        double bestScore = 0d;

        for(JabejaUser partner : users) {
            Long theirPid = partner.getPid();

            int myNeighborsOnMine      = this.getNeighborsOnPartition(pid);
            int myNeighborsOnTheirs    = this.getNeighborsOnPartition(theirPid);
            int theirNeighborsOnMine   = partner.getNeighborsOnPartition(pid);
            int theirNeighborsOnTheirs = partner.getNeighborsOnPartition(theirPid);

            double oldScore = Math.pow(myNeighborsOnMine, alpha) + Math.pow(theirNeighborsOnTheirs, alpha);
            double newScore = Math.pow(myNeighborsOnTheirs, alpha) + Math.pow(theirNeighborsOnMine, alpha);

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partner;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    public int getNeighborsOnPartition(Long pid) {
        int count = 0;
        for(Long friendId : getFriendIDs()) {
            count += manager.getPartitionForUser(friendId).equals(pid) ? 1 : 0;
        }

        return count;
    }

    public Set<JabejaUser> getFriends() {
        return friends;
    }
}
