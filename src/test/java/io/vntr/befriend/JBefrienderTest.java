package io.vntr.befriend;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import org.junit.Test;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class JBefrienderTest {

    @Test
    public void testCalculateGain() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put(2,  initSet(3, 6, 9, 12));
        friendships.put(3,  initSet(4, 8, 12));
        friendships.put(4,  initSet(5, 10));
        friendships.put(5,  initSet(6, 12));
        friendships.put(6,  initSet(7));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        TIntObjectMap<TIntIntMap> uToPToFriendCount = new TIntObjectHashMap<>();

        for(int uid : friendships.keys()) {
            uToPToFriendCount.put(uid, getPToFriendCount(uid, bidirectionalFriendships, uidToPidMap, partitions.keySet()));
        }

        TIntObjectMap<TIntIntMap> expectedResults = new TIntObjectHashMap<>();
        for(int uid : friendships.keys()) {
            expectedResults.put(uid, new TIntIntHashMap());
        }

        for(int uid1 : friendships.keys()) {
            for(int uid2 : friendships.keys()) {
                if(uid1 < uid2) {
                    int pid1 = uidToPidMap.get(uid1);
                    int pid2 = uidToPidMap.get(uid2);
                    if(pid1 < pid2) {
                        boolean friends = bidirectionalFriendships.get(uid1).contains(uid2);
                        int currentScore = uToPToFriendCount.get(uid1).get(pid1) + uToPToFriendCount.get(uid2).get(pid2);
                        int proposedScore = uToPToFriendCount.get(uid1).get(pid2) + uToPToFriendCount.get(uid2).get(pid1);
                        if(friends) {
                            proposedScore -= 2;
                        }
                        expectedResults.get(uid1).put(uid2, proposedScore - currentScore);
                    }
                }
            }
        }

        for(int uid1 : expectedResults.keys()) {
            for(int uid2 : expectedResults.get(uid1).keys()) {
                int expectedResult = expectedResults.get(uid1).get(uid2);
                int result = JBefriender.calculateGain(uid1, uid2, bidirectionalFriendships, uidToPidMap);
                assertTrue(expectedResult == result);
            }
        }
    }

    @Test
    public void findPartner() {
        float alpha = 1f;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12)); //P1:2, P2:2, P3:2
        friendships.put(2,  initSet(3, 6, 9, 12));        //P1:2, P2:2, P3:1
        friendships.put(3,  initSet(4, 8, 12));           //P1:2, P2:1, P3:1
        friendships.put(4,  initSet(5, 10));              //P1:3, P2:0, P3:1
        friendships.put(5,  initSet(6, 12));              //P1:1, P2:1, P3:1
        friendships.put(6,  initSet(7));                  //P1:3, P2:1, P3:0
        friendships.put(7,  initSet(8));                  //P1:0, P2:2, P3:0
        friendships.put(8,  initSet(9));                  //P1:2, P2:2, P3:0
        friendships.put(9,  initSet(10));                 //P1:1, P2:1, P3:1
        friendships.put(10, initSet(11));                 //P1:2, P2:1, P3:1
        friendships.put(11, initSet(12));                 //P1:0, P2:0, P3:2
        friendships.put(12, initSet(13));                 //P1:4, P2:0, P3:2
        friendships.put(13, new TIntHashSet());          //P1:0, P2:0, P3:1

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        Map<Integer, Integer> expectedResults = new HashMap<>();
        expectedResults.put(1, null);
        expectedResults.put(2, null);
        expectedResults.put(3, 6);
        expectedResults.put(4, null);
        expectedResults.put(5, 10);
        expectedResults.put(6, 3);
        expectedResults.put(7, null);
        expectedResults.put(8, null);
        expectedResults.put(9, null);
        expectedResults.put(10, 5);
        expectedResults.put(11, null);
        expectedResults.put(12, null);
        expectedResults.put(13, null);

        for(int uid : friendships.keys()) {
            TIntSet candidates = new TIntHashSet(friendships.keys());
            candidates.removeAll(partitions.get(uidToPidMap.get(uid)));
            Integer result = JBefriender.findPartner(uid, candidates, alpha, bidirectionalFriendships, uidToPidMap);
            Integer expectedResult = expectedResults.get(uid);
            assertEquals(expectedResult, result);
        }
    }

    @Test
    public void testHowManyFriendsHavePartition() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put(1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put(2,  initSet(3, 6, 9, 12));
        friendships.put(3,  initSet(4, 8, 12));
        friendships.put(4,  initSet(5, 10));
        friendships.put(5,  initSet(6, 12));
        friendships.put(6,  initSet(7));
        friendships.put(7,  initSet(8));
        friendships.put(8,  initSet(9));
        friendships.put(9,  initSet(10));
        friendships.put(10, initSet(11));
        friendships.put(11, initSet(12));
        friendships.put(12, initSet(13));
        friendships.put(13, new TIntHashSet());

        TIntObjectMap<TIntSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        TIntObjectMap<TIntIntMap> uToPToFriendCount = new TIntObjectHashMap<>(friendships.size()+1);

        for(int uid : friendships.keys()) {
            uToPToFriendCount.put(uid, getPToFriendCount(uid, bidirectionalFriendships, uidToPidMap, partitions.keySet()));
        }

        for(int uid : friendships.keys()) {
            for(int pid : partitions.keys()) {
                int expectedResult = uToPToFriendCount.get(uid).get(pid);
                int result = JBefriender.howManyFriendsHavePartition(bidirectionalFriendships.get(uid), pid, uidToPidMap);
                assertTrue(expectedResult == result);
            }
        }
    }
}
