package io.vntr.befriend;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.Utils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static io.vntr.befriend.HBefriender.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class HBefrienderTest {

    @Test
    public void testDetermineStrategy() {
        for(int i=0; i<10; i++) {
            assertEquals(determineStrategy(4, 4, 4, (int) (Math.random() * 20), (int) (Math.random() * 20), false, false), NO_CHANGE);
            assertEquals(determineStrategy(4, 5, 4, (int) (Math.random() * 20), (int) (Math.random() * 20), false, false), NO_CHANGE);
            assertEquals(determineStrategy(4, 4, 5, (int) (Math.random() * 20), (int) (Math.random() * 20), false, false), NO_CHANGE);
            assertEquals(determineStrategy(4, 5, 5, (int) (Math.random() * 20), (int) (Math.random() * 20), false, false), NO_CHANGE);
        }

        assertEquals(determineStrategy(5, 4, 5, 2, 3, false, false), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 5, 2, 3, true,  false), NO_CHANGE);
        assertEquals(determineStrategy(5, 4, 5, 5, 5, false, false), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 5, 5, 5, true,  false), NO_CHANGE);
        assertEquals(determineStrategy(5, 4, 5, 3, 3, false, false), NO_CHANGE);

        assertEquals(determineStrategy(5, 4, 4, 2, 3, false, false), LARGE_TO_SMALL);
        assertEquals(determineStrategy(5, 4, 4, 2, 3, true,  false), NO_CHANGE);
        assertEquals(determineStrategy(5, 4, 4, 3, 3, false, false), NO_CHANGE);

        assertEquals(determineStrategy(5, 5, 4, 3, 2, false, false), SMALL_TO_LARGE);
        assertEquals(determineStrategy(5, 5, 4, 3, 2, false, true), NO_CHANGE);
        assertEquals(determineStrategy(5, 5, 4, 5, 5, false, false), SMALL_TO_LARGE);
        assertEquals(determineStrategy(5, 5, 4, 5, 5, false, true), NO_CHANGE);
        assertEquals(determineStrategy(5, 5, 4, 3, 3, false, false), NO_CHANGE);


    }

    @Test
    public void testCalcEdgeCutMove() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20)); //8
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));                 //5
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));                     //4
        friendships.put( 4, initSet( 5, 10, 15));                             //2
        friendships.put( 5, initSet( 6, 12, 18));                             //3
        friendships.put( 6, initSet( 7, 14));                                 //4
        friendships.put( 7, initSet( 8, 16));                                 //1
        friendships.put( 8, initSet( 9, 18));                                 //3
        friendships.put( 9, initSet(10, 20));                                 //2
        friendships.put(10, initSet(11));                                     //3
        friendships.put(11, initSet(12));                                     //1
        friendships.put(12, initSet(13));                                     //4
        friendships.put(13, initSet(14));                                     //0
        friendships.put(14, initSet(15));                                     //2
        friendships.put(15, initSet(16));                                     //3
        friendships.put(16, initSet(17));                                     //4
        friendships.put(17, initSet(18));                                     //0
        friendships.put(18, initSet(19));                                     //4
        friendships.put(19, initSet(20));                                     //0
        friendships.put(20, Collections.<Integer>emptySet());                        //4

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        int cutFrom1 = 22;
        int cutFrom2 = 13;
        int cutFrom3 = 10;
        int cutFrom4 = 11;

        int between1And2 = 8;
        int between1And3 = 7;
        int between1And4 = 7;
        int between2And3 = 2;
        int between2And4 = 3;
        int between3And4 = 1;

        Map<Set<Integer>, Integer> partitionsToExpectedCut = new HashMap<>();
        partitionsToExpectedCut.put(initSet(1, 2), cutFrom1 + cutFrom2 - between1And2);
        partitionsToExpectedCut.put(initSet(1, 3), cutFrom1 + cutFrom3 - between1And3);
        partitionsToExpectedCut.put(initSet(1, 4), cutFrom1 + cutFrom4 - between1And4);
        partitionsToExpectedCut.put(initSet(2, 3), cutFrom2 + cutFrom3 - between2And3);
        partitionsToExpectedCut.put(initSet(2, 4), cutFrom2 + cutFrom4 - between2And4);
        partitionsToExpectedCut.put(initSet(3, 4), cutFrom3 + cutFrom4 - between3And4);

        Map<Integer, Map<Integer, Integer>> uidToPidToFriendCount = new HashMap<>();
        for(int uid : friendships.keySet()) {
            uidToPidToFriendCount.put(uid, getPToFriendCount(uid, bidirectionalFriendships, uidToPidMap, partitions.keySet()));
        }

        for(int uid1 : uidToPidToFriendCount.keySet()) {
            int pid1 = uidToPidMap.get(uid1);
            for(int pid2 : uidToPidToFriendCount.get(uid1).keySet()) {
                if(pid2 <= pid1) {
                    continue;
                }
                int uid2 = partitions.get(pid2).iterator().next();
                int expectedCurrentCut = partitionsToExpectedCut.get(initSet(pid1, pid2));
                int expectedDelta = uidToPidToFriendCount.get(uid1).get(pid2) - uidToPidToFriendCount.get(uid1).get(pid1);
                int expectedScore = expectedCurrentCut - expectedDelta;
                int score = calcEdgeCutMove(uid1, uid2, bidirectionalFriendships, partitions, uidToPidMap);
                assertTrue(score == expectedScore);
            }
        }
    }

    @Test
    public void testGetNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions() {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4,  5));
        partitions.put(2, initSet( 6,  7,  8,  9, 10));
        partitions.put(3, initSet(11, 12, 13, 14, 15));
        partitions.put(4, initSet(16, 17, 18, 19, 20));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put( 1, initSet( 2,  4,  6,  8, 10, 12, 14, 16, 18, 20)); //8
        friendships.put( 2, initSet( 3,  6,  9, 12, 15, 18));                 //5
        friendships.put( 3, initSet( 4,  8, 12, 16, 20));                     //4
        friendships.put( 4, initSet( 5, 10, 15));                             //2
        friendships.put( 5, initSet( 6, 12, 18));                             //3
        friendships.put( 6, initSet( 7, 14));                                 //4
        friendships.put( 7, initSet( 8, 16));                                 //1
        friendships.put( 8, initSet( 9, 18));                                 //3
        friendships.put( 9, initSet(10, 20));                                 //2
        friendships.put(10, initSet(11));                                     //3
        friendships.put(11, initSet(12));                                     //1
        friendships.put(12, initSet(13));                                     //4
        friendships.put(13, initSet(14));                                     //0
        friendships.put(14, initSet(15));                                     //2
        friendships.put(15, initSet(16));                                     //3
        friendships.put(16, initSet(17));                                     //4
        friendships.put(17, initSet(18));                                     //0
        friendships.put(18, initSet(19));                                     //4
        friendships.put(19, initSet(20));                                     //0
        friendships.put(20, Collections.<Integer>emptySet());                        //4

        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        int cutFrom1 = 22;
        int cutFrom2 = 13;
        int cutFrom3 = 10;
        int cutFrom4 = 11;

        int between1And2 = 8;
        int between1And3 = 7;
        int between1And4 = 7;
        int between2And3 = 2;
        int between2And4 = 3;
        int between3And4 = 1;

        Map<Set<Integer>, Integer> partitionsToExpectedCut = new HashMap<>();
        partitionsToExpectedCut.put(initSet(1, 2), cutFrom1 + cutFrom2 - between1And2);
        partitionsToExpectedCut.put(initSet(1, 3), cutFrom1 + cutFrom3 - between1And3);
        partitionsToExpectedCut.put(initSet(1, 4), cutFrom1 + cutFrom4 - between1And4);
        partitionsToExpectedCut.put(initSet(2, 3), cutFrom2 + cutFrom3 - between2And3);
        partitionsToExpectedCut.put(initSet(2, 4), cutFrom2 + cutFrom4 - between2And4);
        partitionsToExpectedCut.put(initSet(3, 4), cutFrom3 + cutFrom4 - between3And4);

        for(int pid1 : partitions.keySet()) {
            for(int pid2 : partitions.keySet()) {
                if(pid1 != pid2) {
                    int expectedCut = partitionsToExpectedCut.get(initSet(pid1, pid2));
                    int cut = getNumberOfEdgesCutThatHaveAtLeastOneUserInOneOfTheseTwoPartitions(pid1, pid2, bidirectionalFriendships, partitions, uidToPidMap);
                    assertTrue(expectedCut == cut);
                }
            }
        }
    }

}
