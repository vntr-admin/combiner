package io.vntr.jabeja;

import io.vntr.utils.ProbabilityUtils;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static io.vntr.jabeja.JabejaTestUtils.*;


/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaManagerTest {

    @Test
    public void testGetRandomSamplingOfUsers() {
        int numUsers = 20;
        int numRuns = 100000;
        double alpha = 1.1D;
        JabejaManager manager = new JabejaManager(alpha, 2D, .2D, 9);
        Long pid1 = manager.addPartition();
        Long pid2 = manager.addPartition();

        for(long id=1; id<=numUsers; id++) {
            manager.addUser(new JabejaUser("User " + id,  id,  id%2 == 1 ? pid1 : pid2, alpha, manager));
        }

        int[] count = new int[numUsers];
        int totalCount = 0;
        for(int i=0; i<numRuns; i++) {
            int howMany = (int)(Math.random() * numUsers);
            Collection<JabejaUser> users = manager.getRandomSamplingOfUsers(howMany);
            totalCount += howMany;
            for(JabejaUser user : users) {
                int intId = user.getId().intValue() - 1;
                count[intId]++;
            }
        }

        double averageCountPerUser = ((double) totalCount) / numUsers;
        double threshold = 0.9 * averageCountPerUser;
        for(int i=0; i<numUsers; i++) {
            assertTrue(count[i] > threshold);
        }
    }


    @Test
    public void testRepartition() {
        int numUsers = 1000;
        double alpha = 1.1D;
        JabejaManager manager = new JabejaManager(alpha, 2D, .2D, 9);
        Long pid1 = manager.addPartition();
        Long pid2 = manager.addPartition();

        for(long id=1; id<=numUsers; id++) {
            manager.addUser(new JabejaUser("User " + id,  id,  id%2 == 1 ? pid1 : pid2, alpha, manager));
        }


        for(long id=1; id<=numUsers; id++) {
            int numFriends = (int)(Math.random() * numUsers * 0.03);
            Collection<JabejaUser> friends = manager.getRandomSamplingOfUsers(numFriends);
            for(JabejaUser friend : friends) {
                manager.befriend(id, friend.getId());
            }
        }

        System.out.println("---1---");
        System.out.println("Edge cut before: " + manager.getEdgeCut());
        manager.repartition();
        System.out.println("Edge cut after:  " + manager.getEdgeCut());
    }

    @Test
    public void testRepartition2() {
        int numUsers = 1000;
        int numPartitions = 10;
        double alpha = 1.1D;
        JabejaManager manager = new JabejaManager(alpha, 2D, .2D, 9);

        List<Long> partitionIds = new ArrayList<Long>(numPartitions+1);
        for(int i=0; i<numPartitions; i++) {
            partitionIds.add(manager.addPartition());
        }

        for(long id=0; id<numUsers; id++) {
            Long pid = ProbabilityUtils.getKDistinctValuesFromList(1, partitionIds).iterator().next();
            manager.addUser(new JabejaUser("User " + id,  id,  pid, alpha, manager));
        }

        Map<Long, Set<Long>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, 20, 0.1, 0.1);
        for(Long userId : friendships.keySet()) {
            for(Long otherUserId : friendships.get(userId)) {
                manager.befriend(userId, otherUserId);
            }
        }

        System.out.println("---2---");
        System.out.println("Edge cut before: " + manager.getEdgeCut());
        manager.repartition();
        System.out.println("Edge cut after:  " + manager.getEdgeCut());
    }



    private static Map<Long, Set<Long>> getTopographyForMultigroupSocialNetwork(int numUsers, int numGroups, double groupMembershipProbability, double intraGroupFriendshipProbability) {
        Map<Long, Set<Long>> userIdToFriendIds = new HashMap<Long, Set<Long>>();
        for(long id=0L; id<numUsers; id++) {
            userIdToFriendIds.put(id, new HashSet<Long>());
        }

        Map<Long, Set<Long>> groupIdToUserIds = new HashMap<Long, Set<Long>>();
        for(long id=0; id<numGroups; id++) {
            groupIdToUserIds.put(id, new HashSet<Long>());
        }

        for(Long userId : userIdToFriendIds.keySet()) {
            for(Long groupId : groupIdToUserIds.keySet()) {
                if(Math.random() < groupMembershipProbability) {
                    groupIdToUserIds.get(groupId).add(userId);
                }
            }
        }

        for(Set<Long> groupMembers : groupIdToUserIds.values()) {
            for(long userId : groupMembers) {
                for(long otherUserId : groupMembers) {
                    if(userId < otherUserId) { //this avoids running it once for each user
                        if(Math.random() < intraGroupFriendshipProbability) {
                            userIdToFriendIds.get(userId).add(otherUserId);
                            userIdToFriendIds.get(otherUserId).add(userId);
                        }
                    }
                }
            }
        }

        return userIdToFriendIds;
    }

    @Test
    public void testGetEdgeCut() {
        double alpha = 1.1D;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(1L, initSet( 1L,  2L,  3L,  4L, 5L));
        partitions.put(2L, initSet( 6L,  7L,  8L,  9L));
        partitions.put(3L, initSet(10L, 11L, 12L, 13L));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        for(Long uid1 = 1L; uid1 <= 13L; uid1++) {
            friendships.put(uid1, new HashSet<Long>());
            for(Long uid2 = 1L; uid2 <= uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        JabejaManager manager = initGraph(alpha, 2D, .2D, 9, partitions, friendships);

        Long expectedCut = 56L; //20 friendships between p1 and p2, same between p1 and p3, and 16 friendships between p2 and p3
        assertEquals(manager.getEdgeCut(), expectedCut);

        //Everybody hates 13
        for(Long uid = 1L; uid <= 12L; uid++) {
            manager.unfriend(uid, 13L);
        }

        expectedCut = 47L; //20 between p1 and p2, 15 between p1 and p3, and 12 between p2 and p3
        assertEquals(manager.getEdgeCut(), expectedCut);
    }
}
