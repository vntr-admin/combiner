package io.vntr.manager;

import io.vntr.utils.InitUtils;
import io.vntr.User;
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;
import static io.vntr.utils.Utils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class NoRepManagerTest {

    @Test
    public void testMoveUser() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Integer uidToMove = 7;
        Integer uidToUnfriend = 8;
        Integer destinationPid = 3;

        //7 and 8 are not friends
        friendships.get(uidToMove).remove(uidToUnfriend);
        friendships.get(uidToUnfriend).remove(uidToMove);

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        for(Integer pid : partitions.keySet()) {
            for(Integer uid : partitions.get(pid)) {
                assertEquals(pid, manager.getUser(uid).getBasePid());
            }
        }

        manager.moveUser(uidToMove, destinationPid, false);

        for(Integer pid : partitions.keySet()) {
            for(Integer uid : partitions.get(pid)) {
                if(uid.equals(uidToMove)) {
                    assertEquals(destinationPid, manager.getUser(uid).getBasePid());
                }
                else {
                    assertEquals(pid, manager.getUser(uid).getBasePid());
                }
            }
        }
    }

    @Test
    public void testAddUser() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);


        //Automatic assignment of user id
        Integer newUid = manager.addUser();
        Set<Integer> expectedUids = new HashSet<>(friendships.keySet());
        expectedUids.add(newUid);
        assertEquals(manager.getUids(), expectedUids);
        assertTrue(manager.getUser(newUid).getFriendIDs().isEmpty());

        assertTrue(manager.getPartition(manager.getUser(newUid).getBasePid()).contains(newUid));

        for(Integer originalUid : friendships.keySet()) {
            assertEquals(manager.getUser(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }


        //Manual specification of user id
        Integer uidToAddManually = new TreeSet<>(manager.getUids()).last()+1;
        manager.addUser(new User(uidToAddManually));
        expectedUids.add(uidToAddManually);
        assertEquals(manager.getUids(), expectedUids);
        assertTrue(manager.getUser(uidToAddManually).getFriendIDs().isEmpty());

        assertTrue(manager.getPartition(manager.getUser(uidToAddManually).getBasePid()).contains(uidToAddManually));

        for(Integer originalUid : friendships.keySet()) {
            assertEquals(manager.getUser(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }
    }

    @Test
    public void testRemoveUser() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Integer uidToRemove = 7;

        friendships.get(uidToRemove).remove(8);
        friendships.get(8).remove(uidToRemove);

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        assertEquals(bidirectionalFriendships, manager.getFriendships());

        Integer pidForUserToRemove = manager.getUser(uidToRemove).getBasePid();
        manager.removeUser(uidToRemove);

        for(Integer pid : partitions.keySet()) {
            if(pid.equals(pidForUserToRemove)) {
                Set<Integer> expectedUsers = new HashSet<>(partitions.get(pidForUserToRemove));
                expectedUsers.remove(uidToRemove);
                assertEquals(manager.getPartition(pidForUserToRemove), expectedUsers);
            }
            else {
                assertEquals(manager.getPartition(pid), partitions.get(pid));
            }
        }

        for(Integer uid : manager.getUids()) {
            assertFalse(manager.getUser(uid).getFriendIDs().contains(uidToRemove));
        }
    }

    @Test
    public void testBefriend() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Integer notFriend1Id = 7;
        Integer notFriend2Id = 8;

        //7 and 8 are not friends
        friendships.get(notFriend1Id).remove(notFriend2Id);
        friendships.get(notFriend2Id).remove(notFriend1Id);

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        for(Integer uid1 : manager.getUids()) {
            for(Integer uid2 : manager.getUids()) {
                if(!uid1.equals(uid2)) {
                    if((uid1.equals(notFriend1Id) && uid2.equals(notFriend2Id)) || (uid1.equals(notFriend2Id) && uid2.equals(notFriend1Id))) {
                        assertFalse(manager.getUser(uid1).getFriendIDs().contains(uid2));
                    }
                    else {
                        assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
                    }
                }
            }
        }

        manager.befriend(notFriend1Id, notFriend2Id);

        for(Integer uid1 : manager.getUids()) {
            for(Integer uid2 : manager.getUids()) {
                if(!uid1.equals(uid2)) {
                    assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
                }
            }
        }
    }

    @Test
    public void testUnfriend() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Integer notFriend1Id = 7;
        Integer notFriend2Id = 8;

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        for(Integer uid1 : manager.getUids()) {
            for(Integer uid2 : manager.getUids()) {
                if(!uid1.equals(uid2)) {
                    assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
                }
            }
        }

        manager.unfriend(notFriend1Id, notFriend2Id);

        for(Integer uid1 : manager.getUids()) {
            for(Integer uid2 : manager.getUids()) {
                if(!uid1.equals(uid2)) {
                    if((uid1.equals(notFriend1Id) && uid2.equals(notFriend2Id)) || (uid1.equals(notFriend2Id) && uid2.equals(notFriend1Id))) {
                        assertFalse(manager.getUser(uid1).getFriendIDs().contains(uid2));
                    }
                    else {
                        assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
                    }
                }
            }
        }
    }

    @Test
    public void testAddPartition() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        assertEquals(manager.getPids(), partitions.keySet());

        Integer newPid = manager.addPartition();

        Set<Integer> expectedPids = new HashSet<>(partitions.keySet());
        expectedPids.add(newPid);

        assertEquals(manager.getPids(), expectedPids);

        Integer pidToAdd = 1;
        for(; pidToAdd < 10; pidToAdd++) {
            if(!manager.getPids().contains(pidToAdd)) {
                break;
            }
        }

        expectedPids.add(pidToAdd);
        manager.addPartition(pidToAdd);
        assertEquals(manager.getPids(), expectedPids);
    }

    @Test
    public void testRemovePartition() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        assertEquals(manager.getPids(), partitions.keySet());

        Integer pidToRemove = 2;

        manager.removePartition(pidToRemove);

        assertEquals(initSet(1, 3), manager.getPids());
        assertEquals(manager.getUids(), friendships.keySet());
        assertEquals(manager.getPartition(1), partitions.get(1));
        assertEquals(manager.getPartition(3), partitions.get(3));

        //Note that manager.removePartition does not migrate!  It is the middleware that does this.
        for(Integer uid : partitions.get(pidToRemove)) {
            Integer userSuppliedPid = manager.getUser(uid).getBasePid();
            assertEquals(userSuppliedPid, pidToRemove);
            assertFalse(manager.getPartition(1).contains(uid) || manager.getPartition(3).contains(uid));
        }
    }

    @Test
    public void testGetEdgeCut() {
        float alpha = 1.1f;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        Integer expectedCut = 56; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3
        assertEquals(manager.getEdgeCut(), expectedCut);

        //Everybody hates 13
        for(Integer uid = 1; uid <= 12; uid++) {
            manager.unfriend(uid, 13);
        }

        expectedCut = 47; //20 between p1 and p2, 15 between p1 and p3, and 12 between p2 and p3
        assertEquals(manager.getEdgeCut(), expectedCut);
    }
}
