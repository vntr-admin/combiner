package io.vntr.jabeja;

import io.vntr.utils.ProbabilityUtils;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static io.vntr.jabeja.JabejaTestUtils.*;
import static io.vntr.TestUtils.*;


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
        Integer pid1 = manager.addPartition();
        Integer pid2 = manager.addPartition();

        for(int id=1; id<=numUsers; id++) {
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
        Integer pid1 = manager.addPartition();
        Integer pid2 = manager.addPartition();

        for(int id=1; id<=numUsers; id++) {
            manager.addUser(new JabejaUser("User " + id,  id,  id%2 == 1 ? pid1 : pid2, alpha, manager));
        }


        for(int id=1; id<=numUsers; id++) {
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

        List<Integer> partitionIds = new ArrayList<Integer>(numPartitions+1);
        for(int i=0; i<numPartitions; i++) {
            partitionIds.add(manager.addPartition());
        }

        for(int id=0; id<numUsers; id++) {
            Integer pid = ProbabilityUtils.getKDistinctValuesFromList(1, partitionIds).iterator().next();
            manager.addUser(new JabejaUser("User " + id,  id,  pid, alpha, manager));
        }

        Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, 20, 0.1, 0.1);
        for(Integer userId : friendships.keySet()) {
            for(Integer otherUserId : friendships.get(userId)) {
                manager.befriend(userId, otherUserId);
            }
        }

        System.out.println("---2---");
        System.out.println("Edge cut before: " + manager.getEdgeCut());
        manager.repartition();
        System.out.println("Edge cut after:  " + manager.getEdgeCut());
    }

    @Test
    public void testGetEdgeCut() {
        double alpha = 1.1D;
        Map<Integer, Set<Integer>> partitions = new HashMap<Integer, Set<Integer>>();
        partitions.put(1, initSet( 1,  2,  3,  4, 5));
        partitions.put(2, initSet( 6,  7,  8,  9));
        partitions.put(3, initSet(10, 11, 12, 13));

        Map<Integer, Set<Integer>> friendships = new HashMap<Integer, Set<Integer>>();
        for(Integer uid1 = 1; uid1 <= 13; uid1++) {
            friendships.put(uid1, new HashSet<Integer>());
            for(Integer uid2 = 1; uid2 <= uid1; uid2++) {
                friendships.get(uid1).add(uid2);
            }
        }

        JabejaManager manager = initGraph(alpha, 2D, .2D, 9, partitions, friendships);

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
