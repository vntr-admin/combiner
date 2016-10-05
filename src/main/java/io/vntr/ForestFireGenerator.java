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
    private NavigableMap<Integer, Set<Integer>> friendships;
    private Map<Integer, Set<Integer>> bidirectionalFriendshipSet;
    private Map<Integer, Set<Integer>> originalFriendships;
    private Map<Integer, Set<Integer>> newFriendships;
    private Set<Integer> visited;
    private Integer v;

    public ForestFireGenerator(float forward, float backward, NavigableMap<Integer, Set<Integer>> friendships) {
        v = friendships.lastKey() + 1;
        this.friendships = friendships;
        this.friendships.put(v, new HashSet<Integer>());
        originalFriendships = cloneFriendships(this.friendships);
        bidirectionalFriendshipSet = generateBidirectionalFriendshipSet(this.friendships);
        visited = new HashSet<Integer>();
        geoDistX = new GeometricDistribution(forward / (1-forward));
        geoDistY = new GeometricDistribution(forward*backward / (1-(forward*backward)));
    }

    public Integer getV() {
        return v;
    }

    private static Map<Integer, Set<Integer>> cloneFriendships(NavigableMap<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> clone = new HashMap<Integer, Set<Integer>>();
        for(Integer key : friendships.keySet()) {
            clone.put(key, new HashSet<Integer>(friendships.get(key)));
        }
        return clone;
    }

    private static Map<Integer, Set<Integer>> diffFriendships(Map<Integer, Set<Integer>> oldFriendships, Map<Integer, Set<Integer>> newFriendships) {
        Map<Integer, Set<Integer>> diff = new HashMap<Integer, Set<Integer>>();
        for(Integer key : newFriendships.keySet()) {
            if(oldFriendships.containsKey(key)) {
                Set<Integer> setDiff = new HashSet<Integer>(newFriendships.get(key));
                setDiff.removeAll(oldFriendships.get(key));
                diff.put(key, setDiff);
            }
            else {
                diff.put(key, newFriendships.get(key));
            }
        }
        return diff;
    }

    private static Map<Integer, Set<Integer>> generateBidirectionalFriendshipSet(Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> bidirectionalFriendshipSet = new HashMap<Integer, Set<Integer>>();
        for(Integer uid : friendships.keySet()) {
            bidirectionalFriendshipSet.put(uid, new HashSet<Integer>());
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                bidirectionalFriendshipSet.get(uid1).add(uid2);
                bidirectionalFriendshipSet.get(uid2).add(uid1);
            }
        }
        return bidirectionalFriendshipSet;
    }

    public Map<Integer, Set<Integer>> run() {
        Integer w = getKDistinctValuesFromList(1, friendships.keySet()).iterator().next();
        forestFireBefriend(w);
        visited.add(w);

        burn(w);
        return diffFriendships(originalFriendships, friendships);
    }

    private void burn(Integer w) {

        int x = geoDistX.sample();
        int y = geoDistY.sample();
        int numToBefriend = x + y;
        Set<Integer> allFriendsOfW = bidirectionalFriendshipSet.get(w);
        allFriendsOfW.removeAll(visited);
        Set<Integer> links;
        if(allFriendsOfW.size() > numToBefriend) {
            links = getKDistinctValuesFromList(numToBefriend, allFriendsOfW);
        }
        else {
            links = allFriendsOfW;
        }
        for(Integer friend : links) {
            forestFireBefriend(friend);
        }
        visited.addAll(links);
        for(Integer friend : links) {
            burn(friend);
        }
    }

    private void forestFireBefriend(Integer w) {
        Integer smaller = Math.min(v, w);
        Integer larger  = Math.max(v, w);
        friendships.get(smaller).add(larger);
        bidirectionalFriendshipSet.get(v).add(w);
        bidirectionalFriendshipSet.get(w).add(v);
    }
}
