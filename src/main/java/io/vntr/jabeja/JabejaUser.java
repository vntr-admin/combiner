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
    private int k;
    private Long pid;
    private JabejaManager manager;
    private Set<JabejaUser> friends; //TODO: make sure this is synchronized with friendIds

    public JabejaUser(String name, Long id, int k, double alpha, JabejaManager manager) {
        super(name, id);
        this.alpha = alpha;
        this.k = k;
        this.manager = manager;
        friends = new HashSet<JabejaUser>();
    }

    public void sampleAndSwap(double initialT, double deltaT) {
        double t = initialT;
        while(t > 1) { //TODO: find better stopping criteria
            JabejaUser partner = findPartner(friends, t);
            if(partner == null) {
                partner = findPartner(manager.getRandomSamplingOfUsers(k), t);
            }
            if(partner != null) {
                //TODO: swap that this and partner
            }
            t -= deltaT;
        }
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

    public int getNeighborsOnPartition(Long partitionId) {
        int count = 0;

        for(Long friendId : getFriendIDs()) {
            count += manager.getPartitionForUser(friendId).equals(partitionId) ? 1 : 0;
        }

        return count;
    }
}
