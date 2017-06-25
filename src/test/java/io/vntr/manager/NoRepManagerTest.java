package io.vntr.manager;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.utils.InitUtils;
import io.vntr.User;
import io.vntr.utils.TroveUtils;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class NoRepManagerTest {

    @Test
    public void testMoveUser() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
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

        for(Integer pid : partitions.keys()) {
            for(TIntIterator iter = partitions.get((pid)).iterator(); iter.hasNext(); ) {
                assertEquals(pid, manager.getUser(iter.next()).getBasePid());
            }
        }

        manager.moveUser(uidToMove, destinationPid, false);

        for(Integer pid : partitions.keys()) {
            for(TIntIterator iter = partitions.get((pid)).iterator(); iter.hasNext(); ) {
                int uid = iter.next();
                int basePid = manager.getUser(uid).getBasePid();
                if(uid == uidToMove) {
                    assertTrue(destinationPid == basePid);
                }
                else {
                    assertTrue(pid == basePid);
                }
            }
        }
    }

    @Test
    public void testAddUser() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4, 5));
        partitions.put(2, TroveUtils.initSet( 6,  7,  8,  9));
        partitions.put(3, TroveUtils.initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);
        TIntObjectMap<TIntSet> bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(friendships);


        //Automatic assignment of user id
        Integer newUid = manager.addUser();
        TIntSet expectedUids = new TIntHashSet(friendships.keySet());
        expectedUids.add(newUid);
        assertEquals(new TIntHashSet(manager.getUids()), expectedUids);
        assertTrue(manager.getUser(newUid).getFriendIDs().isEmpty());

        assertTrue(manager.getPartition(manager.getUser(newUid).getBasePid()).contains(newUid));

        for(Integer originalUid : friendships.keys()) {
            assertEquals(manager.getUser(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }


        //Manual specification of user id
        Integer uidToAddManually = TroveUtils.max(manager.getUids())+1;
        manager.addUser(new User(uidToAddManually));
        expectedUids.add(uidToAddManually);
        assertEquals(new TIntHashSet(manager.getUids()), expectedUids);
        assertTrue(manager.getUser(uidToAddManually).getFriendIDs().isEmpty());

        assertTrue(manager.getPartition(manager.getUser(uidToAddManually).getBasePid()).contains(uidToAddManually));

        for(Integer originalUid : friendships.keys()) {
            assertEquals(manager.getUser(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }
    }

    @Test
    public void testRemoveUser() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4, 5));
        partitions.put(2, TroveUtils.initSet( 6,  7,  8,  9));
        partitions.put(3, TroveUtils.initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Integer uidToRemove = 7;

        friendships.get(uidToRemove).remove(8);
        friendships.get(8).remove(uidToRemove);

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);
        TIntObjectMap<TIntSet> bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(friendships);

        assertEquals(bidirectionalFriendships, manager.getFriendships());

        Integer pidForUserToRemove = manager.getUser(uidToRemove).getBasePid();
        manager.removeUser(uidToRemove);

        for(Integer pid : partitions.keys()) {
            if(pid.equals(pidForUserToRemove)) {
                TIntSet expectedUsers = new TIntHashSet(partitions.get(pidForUserToRemove));
                expectedUsers.remove(uidToRemove);
                assertEquals(new TIntHashSet(manager.getPartition(pidForUserToRemove)), expectedUsers);
            }
            else {
                assertEquals(new TIntHashSet(manager.getPartition(pid)), partitions.get(pid));
            }
        }

        for(TIntIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            assertFalse(manager.getUser(iter.next()).getFriendIDs().contains(uidToRemove));
        }
    }

    @Test
    public void testBefriend() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4, 5));
        partitions.put(2, TroveUtils.initSet( 6,  7,  8,  9));
        partitions.put(3, TroveUtils.initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
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

        for(TIntIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            int uid1 = iter.next();
            for(TIntIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                int uid2 = iter2.next();
                if(uid1 == uid2) {
                    continue;
                }
                if((uid1 == notFriend1Id && uid2 == notFriend2Id) || (uid1 == notFriend2Id && uid2 == notFriend1Id)) {
                    assertFalse(manager.getUser(uid1).getFriendIDs().contains(uid2));
                }
                else {
                    assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
                }
            }
        }
        manager.befriend(notFriend1Id, notFriend2Id);

        for(TIntIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            int uid1 = iter.next();
            for (TIntIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                int uid2 = iter2.next();
                if(uid1 == uid2) {
                    continue;
                }
                assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
            }
        }
    }

    @Test
    public void testUnfriend() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        Integer notFriend1Id = 7;
        Integer notFriend2Id = 8;

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        for(TIntIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            int uid1 = iter.next();
            for (TIntIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                int uid2 = iter2.next();
                if(uid1 == uid2) {
                    continue;
                }
                assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
            }
        }

        manager.unfriend(notFriend1Id, notFriend2Id);

        for(TIntIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            int uid1 = iter.next();
            for (TIntIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                int uid2 = iter2.next();
                if(uid1 == uid2) {
                    continue;
                }
                if((uid1 == notFriend1Id && uid2 == notFriend2Id) || (uid1 == notFriend2Id && uid2 == notFriend1Id)) {
                    assertFalse(manager.getUser(uid1).getFriendIDs().contains(uid2));
                }
                else {
                    assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
                }
            }
        }
    }

    @Test
    public void testAddPartition() {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4, 5));
        partitions.put(2, TroveUtils.initSet( 6,  7,  8,  9));
        partitions.put(3, TroveUtils.initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        assertEquals(manager.getPids(), partitions.keySet());

        Integer newPid = manager.addPartition();

        TIntSet expectedPids = new TIntHashSet(partitions.keySet());
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
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, TroveUtils.initSet( 1,  2,  3,  4, 5));
        partitions.put(2, TroveUtils.initSet( 6,  7,  8,  9));
        partitions.put(3, TroveUtils.initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
            for(Integer uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        assertEquals(manager.getPids(), partitions.keySet());

        Integer pidToRemove = 2;

        manager.removePartition(pidToRemove);

        assertEquals(TroveUtils.initSet(1, 3), manager.getPids());
        assertEquals(manager.getUids(), friendships.keySet());
        assertEquals(manager.getPartition(1), partitions.get(1));
        assertEquals(manager.getPartition(3), partitions.get(3));

        //Note that manager.removePartition does not migrate!  It is the middleware that does this.
        for(TIntIterator iter = partitions.get(pidToRemove).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            Integer userSuppliedPid = manager.getUser(uid).getBasePid();
            assertEquals(userSuppliedPid, pidToRemove);
            assertFalse(manager.getPartition(1).contains(uid) || manager.getPartition(3).contains(uid));
        }
    }

    @Test
    public void testGetEdgeCut() {
        float alpha = 1.1f;
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TIntHashSet());
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
