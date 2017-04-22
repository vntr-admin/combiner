package io.vntr.sparmes;

import io.vntr.Analyzer;
import io.vntr.ForestFireGenerator;
import io.vntr.TestUtils;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;
import org.apache.log4j.Logger;

import java.util.*;

import static io.vntr.Analyzer.ACTIONS.*;
import static io.vntr.TestUtils.*;
import static io.vntr.sparmes.BEFRIEND_REBALANCE_STRATEGY.*;
import static io.vntr.sparmes.SparmesBefriendingStrategy.determineStrategy;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 10/9/16.
 */
public class SparmesAnalyzer {
    final static Logger logger = Logger.getLogger(SparmesAnalyzer.class);

//    @Test
    public void stressTest() throws Exception {
        for(int i=0; i<1000; i++) {
            int numUsers = 500 + (int) (Math.random() * 2000);
            int numGroups = 6 + (int) (Math.random() * 20);
            float groupProb = 0.03f + (float) (Math.random() * 0.1);
            float friendProb = 0.03f + (float) (Math.random() * 0.1);

            Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, friendProb);

            int usersPerPartition = 50 + (int) (Math.random() * 100);

            Set<Integer> pids = new HashSet<>();
            for (int pid = 0; pid < friendships.size() / usersPerPartition; pid++) {
                pids.add(pid);
            }

            Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
            Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(2, partitions, friendships);

            double assortivity = ProbabilityUtils.calculateAssortivityCoefficient(friendships);
            int numFriendships = 0;
            for (Integer uid : friendships.keySet()) {
                numFriendships += friendships.get(uid).size();
            }

            logger.warn("FOR INTERATION:    " + i);
            logger.warn("numUsers:          " + numUsers);
            logger.warn("numGroups:         " + numGroups);
            logger.warn("groupProb:         " + groupProb);
            logger.warn("friendProb:        " + friendProb);
            logger.warn("usersPerPartition: " + usersPerPartition);
            logger.warn("numFriendships:    " + numFriendships);
            logger.warn("assortivity:       " + assortivity);
            logger.warn("friendships:       " + friendships);
            logger.warn("partitions:        " + partitions);
            logger.warn("replicas:          " + replicas);

            SparmesManager SparmesManager = initSparmesManager(friendships, partitions, replicas);
            SparmesMiddleware SparmesMiddleware = initSparmesMiddleware(SparmesManager);

            Map<Analyzer.ACTIONS, Double> actionsProbability = new HashMap<>();
            actionsProbability.put(ADD_USER,         0.125D);
            actionsProbability.put(REMOVE_USER,      0.05D);
            actionsProbability.put(BEFRIEND,         0.64D);
            actionsProbability.put(UNFRIEND,         0.05D);
            actionsProbability.put(FOREST_FIRE,      0.05D);
            actionsProbability.put(ADD_PARTITION,    0.05D);
            actionsProbability.put(REMOVE_PARTITION, 0.01D);
            actionsProbability.put(DOWNTIME,         0.025D);

            Analyzer.ACTIONS[] script = new Analyzer.ACTIONS[2001];
            for(int j=0; j<script.length-1; j++) {
                script[j] = getActions(actionsProbability);
            }
            script[script.length-1] = DOWNTIME;

            runScriptedTest(SparmesMiddleware,    script);
        }
    }

    void runScriptedTest(SparmesMiddleware middleware, Analyzer.ACTIONS[] script) {
        //TODO: more work on the actual assertions, especially replica-specific ones
        assertTrue(isMiddlewareInAValidState(middleware));
        for(int i=0; i<script.length; i++) {
            Analyzer.ACTIONS action = script[i];
            if(action == ADD_USER) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
                Set<Integer> oldUids = new HashSet<>(middleware.getUserIds());
                logger.warn("(" + i + "): " + ADD_USER + ": pre");
                int newUid = middleware.addUser();
                logger.warn("(" + i + "): " + ADD_USER + ": " + newUid);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(pids, middleware.getPartitionIds());
                oldUids.add(newUid);
                assertEquals(middleware.getUserIds(), oldUids);
                oldFriendships.put(newUid, Collections.<Integer>emptySet());
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == REMOVE_USER) {
                Map<Integer, Set<Integer>> oldPartitions = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
                Set<Integer> oldUids = new HashSet<>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
                int badId = ProbabilityUtils.getRandomElement(middleware.getUserIds());
                logger.warn("(" + i + "): " + REMOVE_USER + ": " + badId);
                middleware.removeUser(badId);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(pids, middleware.getPartitionIds());
                oldUids.remove(badId);
                assertEquals(middleware.getUserIds(), oldUids);
                friendships.remove(badId);
                for(int uid : middleware.getUserIds()) {
                    friendships.get(uid).remove(badId);
                }
                assertEquals(friendships, middleware.getFriendships());
                Map<Integer, Set<Integer>> newPartitions  = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                assertUserRemoved(badId, 2, oldPartitions, oldReplicas, newPartitions, newReplicas);
            }
            if(action == BEFRIEND) {
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
                int[] nonfriendship = getNonFriendship(middleware.getFriendships());
                int uid1 = nonfriendship[0];
                int uid2 = nonfriendship[1];

                int oldPid1 = findKeysForUser(oldMasters, uid1).iterator().next();
                int oldPid2 = findKeysForUser(oldMasters, uid2).iterator().next();
                String befriendFormatStr = "(%4d): " + BEFRIEND + ": %4d (on %2d) <-> %4d (on %2d)";
                String output = String.format(befriendFormatStr, i, uid1, oldPid1, uid2, oldPid2);
                logger.warn(output);
                middleware.befriend(uid1, uid2);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                friendships.get(uid1).add(uid2);
                friendships.get(uid2).add(uid1);
                assertEquals(friendships, middleware.getFriendships());
                Map<Integer, Set<Integer>> newMasters     = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                Map<Integer, Set<Integer>> newFriendships = middleware.getFriendships();
                assertProperBefriending(uid1, uid2, oldMasters, oldReplicas, oldFriendships, newMasters, newReplicas, newFriendships);
            }
            if(action == UNFRIEND) {
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
                int[] friendship = getFriendship(middleware.getFriendships());
                logger.warn("(" + i + "): " + UNFRIEND + ": " + friendship[0] + "<->" + friendship[1]);
                middleware.unfriend(friendship[0], friendship[1]);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                friendships.get(friendship[0]).remove(friendship[1]);
                friendships.get(friendship[1]).remove(friendship[0]);
                assertEquals(friendships, middleware.getFriendships());
                Map<Integer, Set<Integer>> newMasters   = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                Map<Integer, Set<Integer>> newFriendships = middleware.getFriendships();
                assertProperUnfriending(friendship[0], friendship[1], oldMasters, oldReplicas, oldFriendships, newMasters, newReplicas, newFriendships);
            }
            if(action == FOREST_FIRE) {
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
                ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<>(copyMapSet(middleware.getFriendships())));
                Set<Integer> newUsersFriends = generator.run();
                int newUid = generator.getV();
                logger.warn("(" + i + "): " + FOREST_FIRE + ": " + newUid + "<->" + newUsersFriends);
                middleware.addUser(new User(newUid));
                assertTrue(isMiddlewareInAValidState(middleware));
                for(Integer friend : newUsersFriends) {
                    middleware.befriend(newUid, friend);
                }
                assertTrue(isMiddlewareInAValidState(middleware));
                uids.add(newUid);
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                friendships.put(newUid, newUsersFriends);
                for(int friendId : newUsersFriends) {
                    friendships.get(friendId).add(newUid);
                }
//                assertEquals(friendships, middleware.getFriendships());

            }
            if(action == ADD_PARTITION) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<>(middleware.getUserIds());
                Set<Integer> oldPids = new HashSet<>(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + ADD_PARTITION + ": pre");
                int newPid = middleware.addPartition();
                logger.warn("(" + i + "): " + ADD_PARTITION + ": " + newPid);
                assertTrue(isMiddlewareInAValidState(middleware));
                Set<Integer> newPids = new HashSet<>(middleware.getPartitionIds());
                newPids.removeAll(oldPids);
                assertTrue(newPids.size() == 1);
                assertTrue(newPids.contains(newPid));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == REMOVE_PARTITION) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
                int badId = ProbabilityUtils.getRandomElement(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + REMOVE_PARTITION + ": " + badId);
                middleware.removePartition(badId);
                assertTrue(isMiddlewareInAValidState(middleware));
                pids.removeAll(middleware.getPartitionIds());
                assertTrue(pids.size() == 1);
                assertTrue(pids.contains(badId));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == DOWNTIME) {
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + DOWNTIME);
                middleware.broadcastDowntime();
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                assertEquals(oldFriendships, middleware.getFriendships());
            }
        }
    }

    private void assertProperBefriending(int uid1, int uid2, Map<Integer, Set<Integer>> oldMasters, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> oldFriendships, Map<Integer, Set<Integer>> newMasters, Map<Integer, Set<Integer>> newReplicas, Map<Integer, Set<Integer>> newFriendships) {
        int oldPid1 = findKeysForUser(oldMasters, uid1).iterator().next();
        Set<Integer> oldReplicas1 = findKeysForUser(oldReplicas, uid1);
        int oldPid2 = findKeysForUser(oldMasters, uid2).iterator().next();
        Set<Integer> oldReplicas2 = findKeysForUser(oldReplicas, uid2);

        boolean uid1ReplicatedOnPid2 = oldReplicas2.contains(oldPid1);
        boolean uid2ReplicatedOnPid1 = oldReplicas1.contains(oldPid2);
        boolean colocatedMasters = oldPid1 == oldPid2;
        boolean colocatedReplicas = uid1ReplicatedOnPid2 && uid2ReplicatedOnPid1;
        if(!colocatedMasters && !colocatedReplicas) {
//            logger.warn("Here's the haps: oldPid1=" + oldPid1 + ", oldPid2=" + oldPid2 + ", oldReplicas1=" + oldReplicas1 + ", oldReplicas2=" + oldReplicas2);
            int originalNumReplicas = oldReplicas.get(oldPid1).size() + oldReplicas.get(oldPid2).size();

            int stay      = originalNumReplicas + (uid1ReplicatedOnPid2 ? 0 : 1) + (uid2ReplicatedOnPid1 ? 0 : 1);
            int toLarger  = originalNumReplicas + calcDeltaNumReplicasMove(uid1, uid2, oldPid1, oldPid2, oldMasters, oldReplicas, oldFriendships);
            int toSmaller = originalNumReplicas + calcDeltaNumReplicasMove(uid2, uid1, oldPid2, oldPid1, oldMasters, oldReplicas, oldFriendships);

            int smallerMasters = oldMasters.get(oldPid1).size();
            int largerMasters  = oldMasters.get(oldPid2).size();

            BEFRIEND_REBALANCE_STRATEGY strategy = determineStrategy(stay, toSmaller, toLarger, smallerMasters, largerMasters);
            if(strategy == NO_CHANGE) {
                assertEquals(copyMapSetNavigable(oldMasters), copyMapSetNavigable(newMasters));
            } else if (strategy == SMALL_TO_LARGE) {
                NavigableMap<Integer, NavigableSet<Integer>> oldMastersCopy = copyMapSetNavigable(oldMasters);
                oldMastersCopy.get(oldPid1).remove(uid1);
                oldMastersCopy.get(oldPid2).add(uid1);
                assertEquals(oldMastersCopy, copyMapSetNavigable(newMasters));
            } else {
                NavigableMap<Integer, NavigableSet<Integer>> oldMastersCopy = copyMapSetNavigable(oldMasters);
                oldMastersCopy.get(oldPid2).remove(uid2);
                oldMastersCopy.get(oldPid1).add(uid2);
                assertEquals(oldMastersCopy, copyMapSetNavigable(newMasters));
            }
        }
    }

    private static int calcDeltaNumReplicasMove(int mover, int stayer, int sourcePid, int targetPid, Map<Integer, Set<Integer>> masters, Map<Integer, Set<Integer>> replicas, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        //TODO: this is just copied from SpajaAnalyzer
        Set<Integer> friendsOfMoverInSource = new HashSet<>(bidirectionalFriendships.get(mover));
        friendsOfMoverInSource.retainAll(masters.get(sourcePid));
        boolean moverHasFriendMasterOnMovingPartition = !friendsOfMoverInSource.isEmpty();

        Set<Integer> moverReplicas = findKeysForUser(replicas, mover);
        boolean moverHasReplicaOnTargetPartitionAndOnlyMinimumRedundancy = replicas.size() <= 2 && moverReplicas.contains(targetPid);

        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = moverHasFriendMasterOnMovingPartition || moverHasReplicaOnTargetPartitionAndOnlyMinimumRedundancy;

        Set<Integer> replicasToAddInStayingPartition = new HashSet<>(bidirectionalFriendships.get(mover));
        replicasToAddInStayingPartition.removeAll(masters.get(targetPid));
        replicasToAddInStayingPartition.removeAll(replicas.get(targetPid));

        //Find replicas that should be deleted
        boolean moverHasReplicaOnTargetPartition = replicas.get(targetPid).contains(mover);
        int effectiveRedundancyOfMover = findKeysForUser(replicas, mover).size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = moverHasReplicaOnTargetPartition && effectiveRedundancyOfMover > 2;

        Set<Integer> stayersFriendsOnSource = new HashSet<>(bidirectionalFriendships.get(stayer));
        stayersFriendsOnSource.retainAll(masters.get(sourcePid));
        stayersFriendsOnSource.remove(mover);
        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = replicas.get(sourcePid).contains(stayer) && stayersFriendsOnSource.isEmpty() && findKeysForUser(replicas, stayer).size() > 2;

        Set<Integer> replicasInMovingPartitionToDelete = new HashSet<>(bidirectionalFriendships.get(mover));
        replicasInMovingPartitionToDelete.retainAll(replicas.get(sourcePid));
        outer:  for(Iterator<Integer> iter = replicasInMovingPartitionToDelete.iterator(); iter.hasNext(); ) {
            int candidate = iter.next();
            int numReplicas = findKeysForUser(replicas, candidate).size();
            if((numReplicas + (replicasToAddInStayingPartition.contains(candidate) ? 1 : 0)) <= 2) {
                iter.remove();
                continue;
            }
            for(int friendOfCandidate : bidirectionalFriendships.get(candidate)) {
                if(friendOfCandidate == mover) {
                    continue;
                }
                int friendPid = findKeysForUser(masters, friendOfCandidate).iterator().next();
                if(friendPid == sourcePid) {
                    iter.remove();
                    continue outer;
                }
            }
        }

        //Calculate net change
        int numReplicasToAdd = replicasToAddInStayingPartition.size() + (shouldWeAddAReplicaOfMovingUserInMovingPartition ? 1 : 0);
        int numReplicasToDelete = replicasInMovingPartitionToDelete.size() + (shouldWeDeleteReplicaOfMovingUserInStayingPartition ? 1 : 0) + (shouldWeDeleteReplicaOfStayingUserInMovingPartition ? 1 : 0);
        return numReplicasToAdd - numReplicasToDelete;
    }

    private SparmesManager initSparmesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparmesTestUtils.initGraph(2, 1.2f, 3, true, partitions, friendships, replicas);
    }

    private SparmesMiddleware initSparmesMiddleware(SparmesManager manager) {
        return new SparmesMiddleware(manager);
    }

    static Analyzer.ACTIONS getActions(Map<Analyzer.ACTIONS, Double> actionsProbability) {
        double random = Math.random();

        for(Analyzer.ACTIONS action : Analyzer.ACTIONS.values()) {
            if(random < actionsProbability.get(action)) {
                return action;
            }
            random -= actionsProbability.get(action);
        }

        return DOWNTIME;
    }

    public static boolean isMiddlewareInAValidState(SparmesMiddleware middleware) {
        boolean valid = true;
        Set<Integer> pids = new HashSet<>(middleware.getPartitionIds());
        Set<Integer> uids = new HashSet<>(middleware.getUserIds());
        assertTrue(middleware.getNumberOfPartitions() == pids.size());
        assertTrue(middleware.getNumberOfUsers()      == uids.size());

        Map<Integer, Set<Integer>> partitions  = middleware.getPartitionToUserMap();
        Map<Integer, Set<Integer>> friendships = middleware.getFriendships();
        Map<Integer, Set<Integer>> replicas = middleware.getPartitionToReplicaMap();

        assertTrue(pids.equals(partitions.keySet()));
        assertTrue(uids.equals(friendships.keySet()));
        assertTrue(pids.equals(replicas.keySet()));

        //Assert that replicas are consistent with the master in everything except pid
        for(int uid : friendships.keySet()) {
            SparmesUser master = middleware.manager.getUserMasterById(uid);
            Set<Integer> replicaPids = findKeysForUser(replicas, uid);
            for(int replicaPid : replicaPids) {
                SparmesUser replica = middleware.manager.getPartitionById(replicaPid).getReplicaById(uid);
                assertEquals(master.getId(), replica.getId());
                assertEquals(master.getFriendIDs(), replica.getFriendIDs());
                assertEquals(master.getBasePid(), replica.getBasePid());
                assertEquals(master.getReplicaPids(), replica.getReplicaPids());
            }
        }

        Set<Integer> allMastersSeen = new HashSet<>();
        for(int pid : partitions.keySet()) {
            allMastersSeen.addAll(partitions.get(pid));
        }
        allMastersSeen.removeAll(middleware.getUserIds());
        assertTrue(allMastersSeen.isEmpty());

        Set<Integer> allReplicasSeen = new HashSet<>();
        for(int pid : replicas.keySet()) {
            allReplicasSeen.addAll(replicas.get(pid));
        }
        allReplicasSeen.removeAll(middleware.getUserIds());
        assertTrue(allReplicasSeen.isEmpty());

        Set<Integer> allFriendsSeen = new HashSet<>();
        for(int pid : friendships.keySet()) {
            allFriendsSeen.addAll(friendships.get(pid));
        }
        allFriendsSeen.removeAll(middleware.getUserIds());
        assertTrue(allFriendsSeen.isEmpty());

        Set<Integer> problematicUids = new HashSet<>();
        int numProblems = 0;
        int count = 0;
        for(int uid : uids) {
            try {
                assertTrue(findKeysForUser(partitions, uid).size() == 1);
                assertTrue(findKeysForUser(replicas, uid).size() >= middleware.manager.getMinNumReplicas());
                count++;
            } catch(AssertionError e) {
                problematicUids.add(uid);
            }
        }
        if(!problematicUids.isEmpty()) {
            throw new RuntimeException("Encountered k-redundancy failures with " + new TreeSet<>(problematicUids));
        }

        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                int pid1 = findKeysForUser(partitions, uid1).iterator().next();
                int pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    //If they aren't colocated, they have replicas in each other's partitions
                    assertTrue(findKeysForUser(replicas, uid1).contains(pid2));
                    assertTrue(findKeysForUser(replicas, uid2).contains(pid1));
                }
            }
        }

        for(int uid : uids) {
            int pid = findKeysForUser(partitions, uid).iterator().next();
            Set<Integer> reps = findKeysForUser(replicas, uid);
            if(reps.contains(pid)) {
                logger.warn(uid + " has a master in " + pid + " and replicas in " + reps);
                valid = false;
            }
        }

        return valid;
    }

    private static void assertUserRemoved(int uid, int minNumReplicas, Map<Integer, Set<Integer>> oldPartitions, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> newPartitions, Map<Integer, Set<Integer>> newReplicas) {
        assertTrue(findKeysForUser(oldPartitions,  uid).size() == 1);
        assertTrue(findKeysForUser(oldReplicas,    uid).size() >= minNumReplicas);
        assertTrue(findKeysForUser(newPartitions,  uid).size() == 0);
        assertTrue(findKeysForUser(newReplicas,    uid).size() == 0);
    }

    private static void assertProperUnfriending(int uid1, int uid2, Map<Integer, Set<Integer>> oldMasters, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> oldFriendships, Map<Integer, Set<Integer>> newMasters, Map<Integer, Set<Integer>> newReplicas, Map<Integer, Set<Integer>> newFriendships) {
        int minUid = Math.min(uid1, uid2);
        int maxUid = Math.max(uid1, uid2);
        assertTrue(oldFriendships.get(minUid).contains(maxUid));
        assertFalse(newFriendships.get(minUid).contains(maxUid));

        int minPid = findKeysForUser(newMasters, minUid).iterator().next();
        int maxPid = findKeysForUser(newMasters, maxUid).iterator().next();

        if(minPid != maxPid) {
            assertTrue(oldReplicas.get(minPid).contains(maxUid));
            assertTrue(oldReplicas.get(maxPid).contains(minUid));

            Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(newFriendships);

            boolean minUidHasExtraReplicas = findKeysForUser(oldReplicas, minUid).size() > 2;
            boolean maxUidHasExtraReplicas = findKeysForUser(oldReplicas, maxUid).size() > 2;

            Set<Integer> friendsOfMaxUidOnMinPid = new HashSet<>(bidirectionalFriendships.get(maxUid));
            friendsOfMaxUidOnMinPid.retainAll(oldMasters.get(minPid));
            friendsOfMaxUidOnMinPid.remove(minUid);
            boolean maxUidHasOtherFriendsOnMinPid = !friendsOfMaxUidOnMinPid.isEmpty();

            Set<Integer> friendsOfMinUidOnMaxPid = new HashSet<>(bidirectionalFriendships.get(minUid));
            friendsOfMinUidOnMaxPid.retainAll(oldMasters.get(maxPid));
            friendsOfMinUidOnMaxPid.remove(maxUid);
            boolean minUidHasOtherFriendsOnMaxPid = !friendsOfMinUidOnMaxPid.isEmpty();

            boolean minUidShouldHaveReplicaOnMaxPid = !minUidHasExtraReplicas || minUidHasOtherFriendsOnMaxPid;
            boolean maxUidShouldHaveReplicaOnMinPid = !maxUidHasExtraReplicas || maxUidHasOtherFriendsOnMinPid;
            assertTrue(maxUidShouldHaveReplicaOnMinPid == newReplicas.get(minPid).contains(maxUid));
            assertTrue(minUidShouldHaveReplicaOnMaxPid == newReplicas.get(maxPid).contains(minUid));
        }
    }
}
