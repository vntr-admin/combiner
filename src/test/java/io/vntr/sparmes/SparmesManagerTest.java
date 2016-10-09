package io.vntr.sparmes;

import io.vntr.ForestFireGenerator;
import io.vntr.TestUtils;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;

/**
 * Created by robertlindquist on 10/5/16.
 */
public class SparmesManagerTest {
    private static final int A_MILLION = 1000000;
    private static final int A_BILLION = 1000000000;
    private static final int MIN_NUM_REPLICAS = 2;

//    @Test
    public void testSomeStuff() throws Exception {
        for(int i=0; i<1000; i++) {
            int numUsers = 900;
            int numGroups = 12;
            float groupProb = 0.08f;
            float friendProb = 0.08f;
            Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, friendProb);

            int usersPerPartition = 50 + (int) (Math.random() * 100);

            Set<Integer> pids = new HashSet<Integer>();
            for(int pid = 0; pid < friendships.size() / usersPerPartition; pid++) {
                pids.add(pid);
            }

            Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
            Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, friendships);

            double assortivity = ProbabilityUtils.calculateAssortivityCoefficient(friendships);
            int numFriendships = 0;
            for(Integer uid : friendships.keySet()) {
                numFriendships += friendships.get(uid).size();
            }

            System.out.println("numUsers:          " + numUsers);
            System.out.println("numGroups:         " + numGroups);
            System.out.println("groupProb:         " + groupProb);
            System.out.println("friendProb:        " + friendProb);
            System.out.println("usersPerPartition: " + usersPerPartition);
            System.out.println("numFriendships:    " + numFriendships);
            System.out.println("assortivity:       " + assortivity);
            System.out.println("friendships:       " + friendships);
            System.out.println("partitions:        " + partitions);
            System.out.println("replicas:          " + replicas);

            SparmesManager sparmesManager = initSparmesManager(friendships, partitions, replicas);
            SparmesMiddleware sparmesMiddleware = initSparmesMiddleware(sparmesManager);

            ForestFireGenerator generator = new ForestFireGenerator(.17f, .17f, new TreeMap<Integer, Set<Integer>>(friendships));
            Set<Integer> newFriendships = generator.run();
            runTest(sparmesMiddleware, generator.getV(), newFriendships);
        }
    }

    private static void runTest(SparmesMiddleware t, int newUid, Set<Integer> newFriendships) {
        long start = System.nanoTime();
        System.out.println("Beginning");
        System.out.println("\tEdge cut: " + t.getEdgeCut());
        System.out.println("\tReplication: " + t.getReplicationCount());


        Map<Integer, Set<Integer>> originalPartitions = t.getPartitionToUserMap();

        t.addUser(new User(newUid));
        for (Integer uid1 : newFriendships) {
            t.befriend(uid1, newUid);
        }

        Map<Integer, Set<Integer>> updatedPartitions = t.getPartitionToUserMap();

        long middle = System.nanoTime() - start;
        long middleSeconds = middle / A_BILLION;
        long middleMilliseconds = (middle % A_BILLION) / A_MILLION;
        System.out.println("After adding a user and " + newFriendships.size() + " new friendships (T+" + middleSeconds + "." + middleMilliseconds + "s)");
        System.out.println("\tEdge cut: " + t.getEdgeCut());
        System.out.println("\tReplication: " + t.getReplicationCount());

        t.broadcastDowntime();

        long end = System.nanoTime() - start;
        long endSeconds = end / A_BILLION;
        long endMilliseconds = (end % A_BILLION) / A_MILLION;
        System.out.println("After broadcasting (T+" + endSeconds + "." + endMilliseconds + "s)");
        System.out.println("\tEdge cut: " + t.getEdgeCut());
        System.out.println("\tReplication: " + t.getReplicationCount());
    }

    private SparmesManager initSparmesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparmesTestUtils.initGraph(2, 1.2f, true, partitions, friendships, replicas);
    }

    private SparmesMiddleware initSparmesMiddleware(SparmesManager manager) {
        return new SparmesMiddleware(manager);
    }
}
