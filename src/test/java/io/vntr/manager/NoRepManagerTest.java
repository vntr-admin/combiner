package io.vntr.manager;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.utils.InitUtils;
import io.vntr.User;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class NoRepManagerTest {

    @Test
    public void testMoveUser() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        short uidToMove = 7;
        short uidToUnfriend = 8;
        short destinationPid = 3;

        //7 and 8 are not friends
        friendships.get(uidToMove).remove(uidToUnfriend);
        friendships.get(uidToUnfriend).remove(uidToMove);

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        for(short pid : partitions.keys()) {
            for(TShortIterator iter = partitions.get((pid)).iterator(); iter.hasNext(); ) {
                assertEquals(pid, manager.getUser(iter.next()).getBasePid());
            }
        }

        manager.moveUser(uidToMove, destinationPid, false);

        for(short pid : partitions.keys()) {
            for(TShortIterator iter = partitions.get((pid)).iterator(); iter.hasNext(); ) {
                short uid = iter.next();
                short basePid = manager.getUser(uid).getBasePid();
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
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2, initSet( 6,  7,  8,  9));
        partitions.put((short)3, initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);


        //Automatic assignment of user id
        short newUid = manager.addUser();
        TShortSet expectedUids = new TShortHashSet(friendships.keySet());
        expectedUids.add(newUid);
        assertEquals(new TShortHashSet(manager.getUids()), expectedUids);
        assertTrue(manager.getUser(newUid).getFriendIDs().isEmpty());

        assertTrue(manager.getPartition(manager.getUser(newUid).getBasePid()).contains(newUid));

        for(short originalUid : friendships.keys()) {
            assertEquals(manager.getUser(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }


        //Manual specification of user id
        short uidToAddManually = (short)(max(manager.getUids())+1);
        manager.addUser(new User(uidToAddManually));
        expectedUids.add(uidToAddManually);
        assertEquals(new TShortHashSet(manager.getUids()), expectedUids);
        assertTrue(manager.getUser(uidToAddManually).getFriendIDs().isEmpty());

        assertTrue(manager.getPartition(manager.getUser(uidToAddManually).getBasePid()).contains(uidToAddManually));

        for(short originalUid : friendships.keys()) {
            assertEquals(manager.getUser(originalUid).getFriendIDs(), bidirectionalFriendships.get(originalUid));
        }
    }

    @Test
    public void testRemoveUser() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2, initSet( 6,  7,  8,  9));
        partitions.put((short)3, initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        short uidToRemove = 7;

        friendships.get(uidToRemove).remove((short)8);
        friendships.get((short)8).remove(uidToRemove);

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        assertEquals(bidirectionalFriendships, manager.getFriendships());

        short pidForUserToRemove = manager.getUser(uidToRemove).getBasePid();
        manager.removeUser(uidToRemove);

        for(short pid : partitions.keys()) {
            if(pid == (pidForUserToRemove)) {
                TShortSet expectedUsers = new TShortHashSet(partitions.get(pidForUserToRemove));
                expectedUsers.remove(uidToRemove);
                assertEquals(new TShortHashSet(manager.getPartition(pidForUserToRemove)), expectedUsers);
            }
            else {
                assertEquals(new TShortHashSet(manager.getPartition(pid)), partitions.get(pid));
            }
        }

        for(TShortIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            assertFalse(manager.getUser(iter.next()).getFriendIDs().contains(uidToRemove));
        }
    }

    @Test
    public void testBefriend() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2, initSet( 6,  7,  8,  9));
        partitions.put((short)3, initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        short notFriend1Id = 7;
        short notFriend2Id = 8;

        //7 and 8 are not friends
        friendships.get(notFriend1Id).remove(notFriend2Id);
        friendships.get(notFriend2Id).remove(notFriend1Id);

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        for(TShortIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            short uid1 = iter.next();
            for(TShortIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                short uid2 = iter2.next();
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

        for(TShortIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            short uid1 = iter.next();
            for (TShortIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                short uid2 = iter2.next();
                if(uid1 == uid2) {
                    continue;
                }
                assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
            }
        }
    }

    @Test
    public void testUnfriend() {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        short notFriend1Id = 7;
        short notFriend2Id = 8;

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        for(TShortIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            short uid1 = iter.next();
            for(TShortIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                short uid2 = iter2.next();
                if(uid1 == uid2) {
                    continue;
                }
                assertTrue(manager.getUser(uid1).getFriendIDs().contains(uid2));
            }
        }

        manager.unfriend(notFriend1Id, notFriend2Id);

        for(TShortIterator iter = manager.getUids().iterator(); iter.hasNext(); ) {
            short uid1 = iter.next();
            for(TShortIterator iter2 = manager.getUids().iterator(); iter2.hasNext(); ) {
                short uid2 = iter2.next();
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
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2, initSet( 6,  7,  8,  9));
        partitions.put((short)3, initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        assertEquals(manager.getPids(), partitions.keySet());

        short newPid = manager.addPartition();

        TShortSet expectedPids = new TShortHashSet(partitions.keySet());
        expectedPids.add(newPid);

        assertEquals(manager.getPids(), expectedPids);

        short pidToAdd = 1;
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
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1, initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2, initSet( 6,  7,  8,  9));
        partitions.put((short)3, initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        assertEquals(manager.getPids(), partitions.keySet());

        short pidToRemove = 2;

        manager.removePartition(pidToRemove);

        assertEquals(initSet(1, 3), manager.getPids());
        assertEquals(manager.getUids(), friendships.keySet());
        assertEquals(manager.getPartition((short)1), partitions.get((short)1));
        assertEquals(manager.getPartition((short)3), partitions.get((short)3));

        //Note that manager.removePartition does not migrate!  It is the middleware that does this.
        for(TShortIterator iter = partitions.get(pidToRemove).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            short userSuppliedPid = manager.getUser(uid).getBasePid();
            assertEquals(userSuppliedPid, pidToRemove);
            assertFalse(manager.getPartition((short)1).contains(uid) || manager.getPartition((short)3).contains(uid));
        }
    }

    @Test
    public void testGetEdgeCut() {
        float alpha = 1.1f;
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>();
        partitions.put((short)1,  initSet( 1,  2,  3,  4, 5));
        partitions.put((short)2,  initSet( 6,  7,  8,  9));
        partitions.put((short)3,  initSet(10, 11, 12, 13));

        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        for(short uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new TShortHashSet());
            for(short uid2 = 1; uid2 < uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        NoRepManager manager = InitUtils.initNoRepManager(0, true, partitions, friendships);

        int expectedCut = 56; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3
        assertEquals(manager.getEdgeCut(), (Integer) expectedCut);

        //Everybody hates 13
        for(short uid = 1; uid <= 12; uid++) {
            manager.unfriend(uid, (short)13);
        }

        expectedCut = 47; //20 between p1 and p2, 15 between p1 and p3, and 12 between p2 and p3
        assertEquals(manager.getEdgeCut(), (Integer) expectedCut);
    }
}
