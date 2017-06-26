package io.vntr.befriend;

import gnu.trove.map.*;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
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
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put((short)2,  initSet(3, 6, 9, 12));
        friendships.put((short)3,  initSet(4, 8, 12));
        friendships.put((short)4,  initSet(5, 10));
        friendships.put((short)5,  initSet(6, 12));
        friendships.put((short)6,  initSet(7));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        TShortObjectMap<TShortShortMap> uToPToFriendCount = new TShortObjectHashMap<>();

        for(short uid : friendships.keys()) {
            uToPToFriendCount.put(uid, getPToFriendCount(uid, bidirectionalFriendships, uidToPidMap, partitions.keySet()));
        }

        TShortObjectMap<TShortShortMap> expectedResults = new TShortObjectHashMap<>();
        for(short uid : friendships.keys()) {
            expectedResults.put(uid, new TShortShortHashMap());
        }

        for(short uid1 : friendships.keys()) {
            for(short uid2 : friendships.keys()) {
                if(uid1 < uid2) {
                    short pid1 = uidToPidMap.get(uid1);
                    short pid2 = uidToPidMap.get(uid2);
                    if(pid1 < pid2) {
                        boolean friends = bidirectionalFriendships.get(uid1).contains(uid2);
                        int currentScore = uToPToFriendCount.get(uid1).get(pid1) + uToPToFriendCount.get(uid2).get(pid2);
                        int proposedScore = uToPToFriendCount.get(uid1).get(pid2) + uToPToFriendCount.get(uid2).get(pid1);
                        if(friends) {
                            proposedScore -= 2;
                        }
                        expectedResults.get(uid1).put(uid2, (short)(proposedScore - currentScore));
                    }
                }
            }
        }

        for(short uid1 : expectedResults.keys()) {
            for(short uid2 : expectedResults.get(uid1).keys()) {
                int expectedResult = expectedResults.get(uid1).get(uid2);
                int result = JBefriender.calculateGain(uid1, uid2, bidirectionalFriendships, uidToPidMap);
                assertTrue(expectedResult == result);
            }
        }
    }

    @Test
    public void findPartner() {
        float alpha = 1f;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12)); //P1:2, P2:2, P3:2
        friendships.put((short)2,  initSet(3, 6, 9, 12));        //P1:2, P2:2, P3:1
        friendships.put((short)3,  initSet(4, 8, 12));           //P1:2, P2:1, P3:1
        friendships.put((short)4,  initSet(5, 10));              //P1:3, P2:0, P3:1
        friendships.put((short)5,  initSet(6, 12));              //P1:1, P2:1, P3:1
        friendships.put((short)6,  initSet(7));                  //P1:3, P2:1, P3:0
        friendships.put((short)7,  initSet(8));                  //P1:0, P2:2, P3:0
        friendships.put((short)8,  initSet(9));                  //P1:2, P2:2, P3:0
        friendships.put((short)9,  initSet(10));                 //P1:1, P2:1, P3:1
        friendships.put((short)10, initSet(11));                 //P1:2, P2:1, P3:1
        friendships.put((short)11, initSet(12));                 //P1:0, P2:0, P3:2
        friendships.put((short)12, initSet(13));                 //P1:4, P2:0, P3:2
        friendships.put((short)13, new TShortHashSet());          //P1:0, P2:0, P3:1

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        Map<Short, Short> expectedResults = new HashMap<>();
        expectedResults.put((short)1, null);
        expectedResults.put((short)2, null);
        expectedResults.put((short)3, (short)6);
        expectedResults.put((short)4, null);
        expectedResults.put((short)5, (short)10);
        expectedResults.put((short)6, (short)3);
        expectedResults.put((short)7, null);
        expectedResults.put((short)8, null);
        expectedResults.put((short)9, null);
        expectedResults.put((short)10, (short)5);
        expectedResults.put((short)11, null);
        expectedResults.put((short)12, null);
        expectedResults.put((short)13, null);

        for(short uid : friendships.keys()) {
            TShortSet candidates = new TShortHashSet(friendships.keys());
            candidates.removeAll(partitions.get(uidToPidMap.get(uid)));
            Short result = JBefriender.findPartner(uid, candidates, alpha, bidirectionalFriendships, uidToPidMap);
            Short expectedResult = expectedResults.get(uid);
            assertEquals(expectedResult, result);
        }
    }

    @Test
    public void testHowManyFriendsHavePartition() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short)1,  initSet(2, 4, 6, 8, 10, 12));
        friendships.put((short)2,  initSet(3, 6, 9, 12));
        friendships.put((short)3,  initSet(4, 8, 12));
        friendships.put((short)4,  initSet(5, 10));
        friendships.put((short)5,  initSet(6, 12));
        friendships.put((short)6,  initSet(7));
        friendships.put((short)7,  initSet(8));
        friendships.put((short)8,  initSet(9));
        friendships.put((short)9,  initSet(10));
        friendships.put((short)10, initSet(11));
        friendships.put((short)11, initSet(12));
        friendships.put((short)12, initSet(13));
        friendships.put((short)13, new TShortHashSet());

        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        TShortObjectMap<TShortShortMap> uToPToFriendCount = new TShortObjectHashMap<>(friendships.size()+1);

        for(short uid : friendships.keys()) {
            uToPToFriendCount.put(uid, getPToFriendCount(uid, bidirectionalFriendships, uidToPidMap, partitions.keySet()));
        }

        for(short uid : friendships.keys()) {
            for(short pid : partitions.keys()) {
                int expectedResult = uToPToFriendCount.get(uid).get(pid);
                int result = JBefriender.howManyFriendsHavePartition(bidirectionalFriendships.get(uid), pid, uidToPidMap);
                assertTrue(expectedResult == result);
            }
        }
    }
}
