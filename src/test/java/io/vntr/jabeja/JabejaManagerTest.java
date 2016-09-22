package io.vntr.jabeja;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

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
            Assert.assertTrue(count[i] > threshold);
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

        manager.repartition();
        //TODO: do this
    }

}
