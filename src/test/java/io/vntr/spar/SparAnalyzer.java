package io.vntr.spar;

import io.vntr.*;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import io.vntr.Analyzer.ACTIONS;
import org.apache.log4j.Logger;
import org.junit.Test;

import static io.vntr.Analyzer.ACTIONS.*;
import static io.vntr.Analyzer.ACTIONS.ADD_PARTITION;
import static io.vntr.Analyzer.ACTIONS.REMOVE_PARTITION;
import static io.vntr.TestUtils.copyMapSet;
import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static io.vntr.TestUtils.findKeysForUser;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 10/7/16.
 */
public class SparAnalyzer {
    final static Logger logger = Logger.getLogger(SparAnalyzer.class);

    private static void assertMiddlewareIsInAValidState(SparMiddleware middleware, int minNumReplicas) {
        Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
        Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
        assertTrue(middleware.getNumberOfPartitions().intValue() == pids.size());
        assertTrue(middleware.getNumberOfUsers().intValue()      == uids.size());

        Map<Integer, Set<Integer>> partitions  = middleware.getPartitionToUserMap();
        Map<Integer, Set<Integer>> replicas    = middleware.getPartitionToReplicaMap();
        Map<Integer, Set<Integer>> friendships = middleware.getFriendships();

        assertEquals(pids, partitions.keySet());
        assertEquals(pids, replicas.keySet());
        assertEquals(uids, friendships.keySet());

        for(int uid : uids) {
            assertTrue(findKeysForUser(partitions, uid).size() == 1);
            assertTrue(findKeysForUser(replicas, uid).size() >= minNumReplicas);
        }

        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                int pid1 = findKeysForUser(partitions, uid1).iterator().next();
                int pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    try {
                        //If they aren't colocated, they have replicas in each other's partitions
                        assertTrue(findKeysForUser(replicas, uid1).contains(pid2));
                        assertTrue(findKeysForUser(replicas, uid2).contains(pid1));
                    } catch(AssertionError e) {
                        throw e;
                    }
                }
            }
        }

        //Assert that replicas are consistent with the master in everything except partitionId
        for(int uid : friendships.keySet()) {
            SparUser master = middleware.manager.getUserMasterById(uid);
            Set<Integer> replicaPids = findKeysForUser(replicas, uid);
            for(int replicaPid : replicaPids) {
                SparUser replica = middleware.manager.getPartitionById(replicaPid).getReplicaById(uid);
                assertEquals(master.getId(), replica.getId());
                assertEquals(master.getFriendIDs(), replica.getFriendIDs());
                assertEquals(master.getMasterPartitionId(), replica.getMasterPartitionId());
                assertEquals(master.getReplicaPartitionIds(), replica.getReplicaPartitionIds());
                assertEquals(master.getPartitionId(), master.getMasterPartitionId());
                assertEquals(replica.getPartitionId(), (Integer) replicaPid);
            }
        }

        Set<Integer> allMastersSeen = new HashSet<Integer>();
        for(int pid : partitions.keySet()) {
            allMastersSeen.addAll(partitions.get(pid));
        }
        allMastersSeen.removeAll(middleware.getUserIds());
        assertTrue(allMastersSeen.isEmpty());

        Set<Integer> allReplicasSeen = new HashSet<Integer>();
        for(int pid : replicas.keySet()) {
            allReplicasSeen.addAll(replicas.get(pid));
        }
        allReplicasSeen.removeAll(middleware.getUserIds());
        assertTrue(allReplicasSeen.isEmpty());

        Set<Integer> allFriendsSeen = new HashSet<Integer>();
        for(int pid : friendships.keySet()) {
            allFriendsSeen.addAll(friendships.get(pid));
        }
        allFriendsSeen.removeAll(middleware.getUserIds());
        assertTrue(allFriendsSeen.isEmpty());
    }

    private static void assertUserAdded(int uid, int minNumReplicas, Map<Integer, Set<Integer>> oldPartitions, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> newPartitions, Map<Integer, Set<Integer>> newReplicas) {
        assertTrue(findKeysForUser(oldPartitions, uid).size() == 0);
        assertTrue(findKeysForUser(oldReplicas,   uid).size() == 0);
        assertTrue(findKeysForUser(newPartitions, uid).size() == 1);
        assertTrue(findKeysForUser(newReplicas,   uid).size() == minNumReplicas);
    }

    private static void assertUserRemoved(int uid, int minNumReplicas, Map<Integer, Set<Integer>> oldPartitions, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> newPartitions, Map<Integer, Set<Integer>> newReplicas, Map<Integer, Set<Integer>> newFriendships) {
        assertTrue(findKeysForUser(oldPartitions,  uid).size() == 1);
        assertTrue(findKeysForUser(oldReplicas,    uid).size() >= minNumReplicas);
        assertTrue(findKeysForUser(newPartitions,  uid).size() == 0);
        assertTrue(findKeysForUser(newReplicas,    uid).size() == 0);
    }

    private static void assertProperBefriending(int uid1, int uid2, Map<Integer, Set<Integer>> oldMasters, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> oldFriendships, Map<Integer, Set<Integer>> newMasters, Map<Integer, Set<Integer>> newReplicas, Map<Integer, Set<Integer>> newFriendships) {
        int minUid = Math.min(uid1, uid2);
        int maxUid = Math.max(uid1, uid2);
        assertFalse(oldFriendships.get(minUid).contains(maxUid));
        assertTrue (newFriendships.get(minUid).contains(maxUid));

        int minPid = findKeysForUser(newMasters, minUid).iterator().next();
        int maxPid = findKeysForUser(newMasters, maxUid).iterator().next();

        if(minPid != maxPid) {
            Set<Integer> minPidReplicas = oldReplicas.get(minPid);
            Set<Integer> maxPidReplicas = oldReplicas.get(maxPid);

            if(minPidReplicas.contains(maxUid) && maxPidReplicas.contains(minUid)) {
                //colocated
                return;
            }

            Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(oldFriendships);

            int curNumReplicas = minPidReplicas.size() + maxPidReplicas.size();
            int noChange = curNumReplicas + (maxPidReplicas.contains(minUid) ? 0 : 1) +  + (minPidReplicas.contains(maxUid) ? 0 : 1);
            int minToMax = curNumReplicas + calcDeltaNumReplicasMove(minUid, maxUid, minPid, maxPid, oldMasters, oldReplicas, oldFriendships);
            int maxToMin = curNumReplicas + calcDeltaNumReplicasMove(maxUid, minUid, maxPid, minPid, oldMasters, oldReplicas, oldFriendships);
            int minMasters = oldMasters.get(minPid).size();
            int maxMasters = oldMasters.get(maxPid).size();
            BEFRIEND_REBALANCE_STRATEGY strat = SparBefriendingStrategy.determineStrategy(noChange, maxToMin, minToMax, minMasters, maxMasters);

            if(strat == NO_CHANGE) {
                assertTrue (newMasters.get(maxPid).contains(maxUid));
                assertFalse(newMasters.get(maxPid).contains(minUid));
                assertTrue (newMasters.get(minPid).contains(minUid));
                assertFalse(newMasters.get(minPid).contains(maxUid));
                assertTrue (newReplicas.get(maxPid).contains(minUid));
                assertTrue (newReplicas.get(minPid).contains(maxUid));
            } else if(strat == SMALL_TO_LARGE) {
                assertFalse(newMasters.get(minPid).contains(minUid));
                assertTrue (newMasters.get(maxPid).contains(minUid));
                assertFalse(newReplicas.get(maxPid).contains(minUid));

                Set<Integer> friendsOfMinUidToVerify = new HashSet<Integer>(bidirectionalFriendships.get(minUid));
                friendsOfMinUidToVerify.removeAll(newMasters.get(maxPid));
                friendsOfMinUidToVerify.removeAll(newReplicas.get(maxPid));
                assertTrue(friendsOfMinUidToVerify.isEmpty());
            } else {
                assertFalse(newMasters.get(maxPid).contains(maxUid));
                assertTrue (newMasters.get(minPid).contains(maxUid));
                assertFalse(newReplicas.get(minPid).contains(maxUid));

                Set<Integer> friendsOfMaxUidToVerify = new HashSet<Integer>(bidirectionalFriendships.get(maxUid));
                friendsOfMaxUidToVerify.removeAll(newMasters.get(minPid));
                friendsOfMaxUidToVerify.removeAll(newReplicas.get(minPid));
                assertTrue(friendsOfMaxUidToVerify.isEmpty());
            }
        }
    }

    private static int calcDeltaNumReplicasMove(int mover, int stayer, int sourcePid, int targetPid, Map<Integer, Set<Integer>> masters, Map<Integer, Set<Integer>> replicas, Map<Integer, Set<Integer>> friendships) {
        //Find replicas that need to be added
        Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(friendships);

        Set<Integer> friendsOfMoverInSource = new HashSet<Integer>(bidirectionalFriendships.get(mover));
        friendsOfMoverInSource.retainAll(masters.get(sourcePid));
        boolean shouldWeAddAReplicaOfMovingUserInMovingPartition = !friendsOfMoverInSource.isEmpty();

        Set<Integer> replicasToAddInStayingPartition = new HashSet<Integer>(bidirectionalFriendships.get(mover));
        replicasToAddInStayingPartition.removeAll(masters.get(targetPid));
        replicasToAddInStayingPartition.removeAll(replicas.get(targetPid));

        //Find replicas that should be deleted
        boolean shouldWeDeleteReplicaOfMovingUserInStayingPartition = replicas.get(targetPid).contains(mover) && findKeysForUser(replicas, mover).size() > 2;

        Set<Integer> stayersFriendsOnSource = new HashSet<Integer>(bidirectionalFriendships.get(stayer));
        stayersFriendsOnSource.retainAll(masters.get(sourcePid));
        stayersFriendsOnSource.remove(mover);
        //TODO: this disagrees with the results from SparBefriendingStrategy
        boolean shouldWeDeleteReplicaOfStayingUserInMovingPartition = replicas.get(sourcePid).contains(stayer) && stayersFriendsOnSource.isEmpty() && findKeysForUser(replicas, stayer).size() > 2;

        //TODO: this disagrees with the results from SparBefriendingStrategy
        Set<Integer> replicasInMovingPartitionToDelete = new HashSet<Integer>(bidirectionalFriendships.get(mover));
        replicasInMovingPartitionToDelete.retainAll(replicas.get(sourcePid));
outer:  for(Iterator<Integer> iter = replicasInMovingPartitionToDelete.iterator(); iter.hasNext(); ) {
            int candidate = iter.next();
            int numReplicas = findKeysForUser(replicas, candidate).size();
            if((numReplicas + (replicasToAddInStayingPartition.contains(candidate) ? 1 : 0)) <= 2) {
                iter.remove();
                continue outer;
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

            Set<Integer> friendsOfMaxUidOnMinPid = new HashSet<Integer>(bidirectionalFriendships.get(maxUid));
            friendsOfMaxUidOnMinPid.retainAll(oldMasters.get(minPid));
            friendsOfMaxUidOnMinPid.remove(minUid);
            boolean maxUidHasOtherFriendsOnMinPid = !friendsOfMaxUidOnMinPid.isEmpty();

            Set<Integer> friendsOfMinUidOnMaxPid = new HashSet<Integer>(bidirectionalFriendships.get(minUid));
            friendsOfMinUidOnMaxPid.retainAll(oldMasters.get(maxPid));
            friendsOfMinUidOnMaxPid.remove(maxUid);
            boolean minUidHasOtherFriendsOnMaxPid = !friendsOfMinUidOnMaxPid.isEmpty();

            boolean minUidShouldHaveReplicaOnMaxPid = !minUidHasExtraReplicas || minUidHasOtherFriendsOnMaxPid;
            boolean maxUidShouldHaveReplicaOnMinPid = !maxUidHasExtraReplicas || maxUidHasOtherFriendsOnMinPid;
            assertTrue(maxUidShouldHaveReplicaOnMinPid == newReplicas.get(minPid).contains(maxUid));
            assertTrue(minUidShouldHaveReplicaOnMaxPid == newReplicas.get(maxPid).contains(minUid));
        }
    }

    private static void assertProperPartitionRemoval(int pid, Map<Integer, Set<Integer>> oldMasters, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> newMasters, Map<Integer, Set<Integer>> newReplicas) {
        assertTrue(oldMasters.containsKey(pid));
        assertTrue(oldReplicas.containsKey(pid));
        assertFalse(newMasters.containsKey(pid));
        assertFalse(newReplicas.containsKey(pid));
        for(int movedMasterId : oldMasters.get(pid)) {
            assertTrue(findKeysForUser(newMasters,  movedMasterId).size() == 1);
            assertTrue(findKeysForUser(newReplicas, movedMasterId).size() >= 2);
        }
        for(int movedReplicaId : oldReplicas.get(pid)) {
            assertTrue(findKeysForUser(newReplicas, movedReplicaId).size() >= 2);
        }
    }

    private static int[] getNonFriendship(Map<Integer, Set<Integer>> friendships) {
        Integer userWhoIsntFriendsWithEveryone = null;
        while(userWhoIsntFriendsWithEveryone == null) {
            int chosenOne = ProbabilityUtils.getRandomElement(friendships.keySet());
            if(friendships.get(chosenOne).size() < friendships.size() - 1) {
                userWhoIsntFriendsWithEveryone = chosenOne;
            }
        }
        Set<Integer> nonfriends = new HashSet<Integer>(friendships.keySet());
        nonfriends.removeAll(friendships.get(userWhoIsntFriendsWithEveryone));
        int friend = ProbabilityUtils.getRandomElement(nonfriends);
        return new int[] {userWhoIsntFriendsWithEveryone, friend};
    }

    private static int[] getFriendship(Map<Integer, Set<Integer>> friendships) {
        Set<Integer> possibilities = new HashSet<Integer>();
        for(int uid : friendships.keySet()) {
            if(!friendships.get(uid).isEmpty()) {
                possibilities.add(uid);
            }
        }

        int chosenOne = ProbabilityUtils.getRandomElement(possibilities);
        return new int[]{chosenOne, ProbabilityUtils.getRandomElement(friendships.get(chosenOne))};
    }

    void runScriptedTest(SparMiddleware middleware, Analyzer.ACTIONS[] script) {
        assertMiddlewareIsInAValidState(middleware, 2);
        for(int i=0; i<script.length; i++) {
            ACTIONS action = script[i];
            if(action == ADD_USER) {
                Map<Integer, Set<Integer>> oldPartitions = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                logger.warn("(" + i + "): " + ADD_USER + ": pre");
                int newUid = middleware.addUser();
                logger.warn("(" + i + "): " + ADD_USER + ": " + newUid);
                assertMiddlewareIsInAValidState(middleware, 2);
                Map<Integer, Set<Integer>> newPartitions = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas   = middleware.getPartitionToReplicaMap();
                assertUserAdded(newUid, 2, oldPartitions, oldReplicas, newPartitions, newReplicas);
            }
            if(action == REMOVE_USER) {
                Map<Integer, Set<Integer>> oldPartitions = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                int badId = ProbabilityUtils.getRandomElement(middleware.getUserIds());
                logger.warn("(" + i + "): " + REMOVE_USER + ": " + badId);
                middleware.removeUser(badId);
                assertMiddlewareIsInAValidState(middleware, 2);
                Map<Integer, Set<Integer>> newPartitions  = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                Map<Integer, Set<Integer>> newFriendships = middleware.getFriendships();
                assertUserRemoved(badId, 2, oldPartitions, oldReplicas, newPartitions, newReplicas, newFriendships);
            }
            if(action == BEFRIEND) {
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());

                int[] nonfriendship = getNonFriendship(middleware.getFriendships());
                int uid1 = nonfriendship[0];
                int uid2 = nonfriendship[1];

                logger.warn("(" + i + "): " + BEFRIEND + ": " + uid1 + "<->" + uid2);
                middleware.befriend(uid1, uid2);
                assertMiddlewareIsInAValidState(middleware, 2);
                Map<Integer, Set<Integer>> newMasters   = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                Map<Integer, Set<Integer>> newFriendships = middleware.getFriendships();
                assertProperBefriending(uid1, uid2, oldMasters, oldReplicas, oldFriendships, newMasters, newReplicas, newFriendships);
            }
            if(action == UNFRIEND) {
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                int[] friendship = getFriendship(middleware.getFriendships());
                logger.warn("(" + i + "): " + UNFRIEND + ": " + friendship[0] + "<->" + friendship[1]);
                middleware.unfriend(friendship[0], friendship[1]);
                assertMiddlewareIsInAValidState(middleware, 2);
                Map<Integer, Set<Integer>> newMasters   = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                Map<Integer, Set<Integer>> newFriendships = middleware.getFriendships();
                assertProperUnfriending(friendship[0], friendship[1], oldMasters, oldReplicas, oldFriendships, newMasters, newReplicas, newFriendships);
            }
            if(action == FOREST_FIRE) {
                ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<Integer, Set<Integer>>(copyMapSet(middleware.getFriendships())));
                Set<Integer> newUsersFriends = generator.run();
                int newUid = generator.getV();
                logger.warn("(" + i + "): " + FOREST_FIRE + ": " + newUid + "<->" + newUsersFriends);
                middleware.addUser(new User(newUid));
                assertMiddlewareIsInAValidState(middleware, 2);
                for(Integer friend : newUsersFriends) {
                    middleware.befriend(newUid, friend);
                }
                assertMiddlewareIsInAValidState(middleware, 2);
            }
            if(action == ADD_PARTITION) {
                Set<Integer> oldPids = new HashSet<Integer>(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + ADD_PARTITION + ": pre");
                int newPid = middleware.addPartition();
                logger.warn("(" + i + "): " + ADD_PARTITION + ": " + newPid);
                assertMiddlewareIsInAValidState(middleware, 2);
                Set<Integer> newPids = new HashSet<Integer>(middleware.getPartitionIds());
                newPids.removeAll(oldPids);
                assertTrue(newPids.size() == 1);
                assertTrue(newPids.contains(newPid));
            }
            if(action == REMOVE_PARTITION) {
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int badId = ProbabilityUtils.getRandomElement(middleware.getPartitionIds());
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                logger.warn("(" + i + "): " + REMOVE_PARTITION + ": " + badId);
                middleware.removePartition(badId);
                assertMiddlewareIsInAValidState(middleware, 2);
                pids.removeAll(middleware.getPartitionIds());
                assertTrue(pids.size() == 1);
                assertTrue(pids.contains(badId));
                Map<Integer, Set<Integer>> newMasters   = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                assertProperPartitionRemoval(badId, oldMasters, oldReplicas, newMasters, newReplicas);
            }
            if(action == DOWNTIME) {
                logger.warn("(" + i + "): " + DOWNTIME);
                middleware.broadcastDowntime();
                assertMiddlewareIsInAValidState(middleware, 2);
            }
        }
    }

//    @Test
    public void stressTest() throws Exception {
        for(int i=0; i<1000; i++) {
            int numUsers = 500 + (int) (Math.random() * 2000);
            int numGroups = 6 + (int) (Math.random() * 20);
            float groupProb = 0.03f + (float) (Math.random() * 0.1);
            float friendProb = 0.03f + (float) (Math.random() * 0.1);
            Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, friendProb);

            int usersPerPartition = 50 + (int) (Math.random() * 100);

            Set<Integer> pids = new HashSet<Integer>();
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

            logger.warn("\nnumUsers:          " + numUsers);
            logger.warn("numGroups:         " + numGroups);
            logger.warn("groupProb:         " + groupProb);
            logger.warn("friendProb:        " + friendProb);
            logger.warn("usersPerPartition: " + usersPerPartition);
            logger.warn("numFriendships:    " + numFriendships);
            logger.warn("assortivity:       " + assortivity);
            logger.warn("friendships:       " + friendships);
            logger.warn("partitions:        " + partitions);
            logger.warn("replicas:          " + replicas);

            SparManager sparManager = initSparManager(friendships, partitions, replicas);
            SparMiddleware sparMiddleware = initSparMiddleware(sparManager);

            Map<ACTIONS, Double> actionsProbability = new HashMap<ACTIONS, Double>();
            actionsProbability.put(ADD_USER,         0.125D);
            actionsProbability.put(REMOVE_USER,      0.05D);
            actionsProbability.put(BEFRIEND,         0.64D);
            actionsProbability.put(UNFRIEND,         0.05D);
            actionsProbability.put(FOREST_FIRE,      0.05D);
            actionsProbability.put(ADD_PARTITION,    0.05D);
            actionsProbability.put(REMOVE_PARTITION, 0.01D);
            actionsProbability.put(DOWNTIME,         0.025D);

            ACTIONS[] script = new ACTIONS[2001];
            for(int j=0; j<script.length-1; j++) {
                script[j] = getActions(actionsProbability);
            }
            script[script.length-1] = DOWNTIME;

            runScriptedTest(sparMiddleware,    script);
        }
    }

    private SparManager initSparManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparTestUtils.initGraph(2, partitions, friendships, replicas);
    }

    private SparMiddleware initSparMiddleware(SparManager manager) {
        return new SparMiddleware(manager);
    }

    static ACTIONS getActions(Map<ACTIONS, Double> actionsProbability) {
        double random = Math.random();

        for(ACTIONS action : ACTIONS.values()) {
            if(random < actionsProbability.get(action)) {
                return action;
            }
            random -= actionsProbability.get(action);
        }

        return DOWNTIME;
    }

}
