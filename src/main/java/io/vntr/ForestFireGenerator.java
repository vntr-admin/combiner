package io.vntr;

import org.apache.commons.math3.distribution.GeometricDistribution;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 10/2/16.
 */
public class ForestFireGenerator {
    private GeometricDistribution geoDistX;
    private GeometricDistribution geoDistY;
    private NavigableMap<Long, Set<Long>> friendships;
    private Map<Long, Set<Long>> bidirectionalFriendshipSet;
    private Map<Long, Set<Long>> originalFriendships;
    private Map<Long, Set<Long>> newFriendships;
    private Set<Long> visited;
    private Long v;

    public ForestFireGenerator(double forward, double backward, NavigableMap<Long, Set<Long>> friendships) {
        v = friendships.lastKey() + 1L;
        this.friendships = friendships;
        this.friendships.put(v, new HashSet<Long>());
        originalFriendships = cloneFriendships(this.friendships);
        bidirectionalFriendshipSet = generateBidirectionalFriendshipSet(this.friendships);
        visited = new HashSet<Long>();
        geoDistX = new GeometricDistribution(forward / (1-forward));
        geoDistY = new GeometricDistribution(forward*backward / (1-(forward*backward)));
    }

    public Long getV() {
        return v;
    }

    private static Map<Long, Set<Long>> cloneFriendships(NavigableMap<Long, Set<Long>> friendships) {
        Map<Long, Set<Long>> clone = new HashMap<Long, Set<Long>>();
        for(Long key : friendships.keySet()) {
            clone.put(key, new HashSet<Long>(friendships.get(key)));
        }
        return clone;
    }

    private static Map<Long, Set<Long>> diffFriendships(Map<Long, Set<Long>> oldFriendships, Map<Long, Set<Long>> newFriendships) {
        Map<Long, Set<Long>> diff = new HashMap<Long, Set<Long>>();
        for(Long key : newFriendships.keySet()) {
            if(oldFriendships.containsKey(key)) {
                Set<Long> setDiff = new HashSet<Long>(newFriendships.get(key));
                setDiff.removeAll(oldFriendships.get(key));
                diff.put(key, setDiff);
            }
            else {
                diff.put(key, newFriendships.get(key));
            }
        }
        return diff;
    }

    private static Map<Long, Set<Long>> generateBidirectionalFriendshipSet(Map<Long, Set<Long>> friendships) {
        Map<Long, Set<Long>> bidirectionalFriendshipSet = new HashMap<Long, Set<Long>>();
        for(Long uid : friendships.keySet()) {
            bidirectionalFriendshipSet.put(uid, new HashSet<Long>());
        }
        for(Long uid1 : friendships.keySet()) {
            for(Long uid2 : friendships.get(uid1)) {
                bidirectionalFriendshipSet.get(uid1).add(uid2);
                bidirectionalFriendshipSet.get(uid2).add(uid1);
            }
        }
        return bidirectionalFriendshipSet;
    }

    public Map<Long, Set<Long>> run() {
        Long w = getKDistinctValuesFromList(1, friendships.keySet()).iterator().next();
        forestFireBefriend(w);
        visited.add(w);

        burn(w);
        return diffFriendships(originalFriendships, friendships);
    }

    private void burn(Long w) {

        int x = geoDistX.sample();
        int y = geoDistY.sample();
        int numToBefriend = x + y;
        Set<Long> allFriendsOfW = bidirectionalFriendshipSet.get(w);
        allFriendsOfW.removeAll(visited);
        Set<Long> links;
        if(allFriendsOfW.size() > numToBefriend) {
            links = getKDistinctValuesFromList(numToBefriend, allFriendsOfW);
        }
        else {
            links = allFriendsOfW;
        }
        for(Long friend : links) {
            forestFireBefriend(friend);
        }
        visited.addAll(links);
        for(Long friend : links) {
            burn(friend);
        }
    }

    private void forestFireBefriend(Long w) {
        Long smaller = Math.min(v, w);
        Long larger  = Math.max(v, w);
        friendships.get(smaller).add(larger);
        bidirectionalFriendshipSet.get(v).add(w);
        bidirectionalFriendshipSet.get(w).add(v);
    }
}
