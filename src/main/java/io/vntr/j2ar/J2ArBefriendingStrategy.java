package io.vntr.j2ar;

import io.vntr.User;

import java.util.HashSet;
import java.util.Set;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 4/18/17.
 */
public class J2ArBefriendingStrategy {

    float alpha;
    int k;
    private J2ArManager manager;

    public J2ArBefriendingStrategy(float alpha, int k, J2ArManager manager) {
        this.alpha = alpha;
        this.k = k;
        this.manager = manager;
    }

    void rebalance(Integer smallerUserId, Integer largerUserId) {
        User smallerUser = manager.getUser(smallerUserId);
        int smallerPid = smallerUser.getBasePid();

        User largerUser = manager.getUser(largerUserId);
        int largerPid = largerUser.getBasePid();

        if (smallerPid != largerPid) {
            User smallerPartner = findPartnerOnPartition(smallerUser, largerPid);
            User largerPartner = findPartnerOnPartition(largerUser, smallerPid);

            if(smallerPartner != null && largerPartner == null) {
                manager.moveUser(smallerUserId, largerPid, false);
                manager.moveUser(smallerPartner.getId(), smallerPid, false);
            }
            else if(largerPartner != null && smallerPartner == null) {
                manager.moveUser(largerUserId, smallerPid, false);
                manager.moveUser(largerPartner.getId(), largerPid, false);
            }
            else if(smallerPartner != null && largerPartner != null) {
                int gainSmallerToLarger = calculateGain(smallerPartner, largerPartner);
                int gainLargerToSmaller = calculateGain(largerPartner, smallerPartner);
                if(gainSmallerToLarger >= gainLargerToSmaller) {
                    manager.moveUser(smallerUserId, largerPid, false);
                    manager.moveUser(smallerPartner.getId(), smallerPid, false);
                }
                else {
                    manager.moveUser(largerUserId, smallerPid, false);
                    manager.moveUser(largerPartner.getId(), largerPid, false);
                }
            }
        }
    }

    User findPartnerOnPartition(User user, Integer pid) {
        Set<Integer> partition = manager.getPartition(pid);
        Set<Integer> candidates;
        if(partition.size() <= k) {
            candidates = new HashSet<>(partition);
        }
        else {
            candidates = getKDistinctValuesFromList(k, partition);
        }
        return findPartner(user, manager.getUsers(candidates), 1F);
    }

    int calculateGain(User user1, User user2) {
        int oldCut = getNeighborsOnPartition(user1.getId(), user2.getBasePid()) + getNeighborsOnPartition(user2.getId(), user1.getBasePid());
        int newCut = getNeighborsOnPartition(user1.getId(), user1.getBasePid()) + getNeighborsOnPartition(user2.getId(), user2.getBasePid());
        return oldCut - newCut;
    }

    public int getNeighborsOnPartition(Integer uid, Integer pid) {
        int count = 0;
        for(Integer friendId : manager.getUser(uid).getFriendIDs()) {
            count += manager.getUser(friendId).getBasePid().equals(pid) ? 1 : 0;
        }

        return count;
    }

    User findPartner(User user, Set<User> candidates, float t) {
        User bestPartner = null;
        float bestScore = 0f;

        Integer myPid = user.getBasePid();

        for(User partner : candidates) {
            Integer theirPid = partner.getBasePid();
            if(theirPid.equals(myPid)) {
                continue;
            }

            int myNeighborsOnMine      = howManyFriendsHavePartition(user, myPid);
            int myNeighborsOnTheirs    = howManyFriendsHavePartition(user, theirPid);
            int theirNeighborsOnMine   = howManyFriendsHavePartition(partner, myPid);
            int theirNeighborsOnTheirs = howManyFriendsHavePartition(partner, theirPid);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, alpha) + Math.pow(theirNeighborsOnTheirs, alpha));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, alpha) + Math.pow(theirNeighborsOnMine, alpha));

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partner;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    int howManyFriendsHavePartition(User user, Integer pid) {
        int count = 0;
        for(Integer friendId : user.getFriendIDs()) {
            User friend = manager.getUser(friendId);
            count += friend.getBasePid().equals(pid) ? 1 : 0;
        }
        return count;
    }
}