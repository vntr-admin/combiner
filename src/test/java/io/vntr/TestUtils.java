package io.vntr;

 import io.vntr.utils.ProbabilityUtils;

 import java.io.File;
 import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 9/24/16.
 */
public class TestUtils {
    public static <T> Set<T> initSet(T... args) {
        Set<T> set = new HashSet<>();
        Collections.addAll(set, args);
        return set;
    }

    public static <T> Set<T> initSet(Set<T> initialSet, T... args) {
        Set<T> set = new HashSet<>();
        set.addAll(initialSet);
        Collections.addAll(set, args);
        return set;
    }

    public static Map<Integer, Set<Integer>> getTopographyForMultigroupSocialNetwork(int numUsers, int numGroups, float groupMembershipProbability, float intraGroupFriendshipProbability) {
        Map<Integer, Set<Integer>> userIdToFriendIds = new HashMap<>();
        for(int id=0; id<numUsers; id++) {
            userIdToFriendIds.put(id, new HashSet<Integer>());
        }

        Map<Integer, Set<Integer>> groupIdToUserIds = new HashMap<>();
        for(int id=0; id<numGroups; id++) {
            groupIdToUserIds.put(id, new HashSet<Integer>());
        }

        for(Integer userId : userIdToFriendIds.keySet()) {
            for(Integer groupId : groupIdToUserIds.keySet()) {
                if(Math.random() < groupMembershipProbability) {
                    groupIdToUserIds.get(groupId).add(userId);
                }
            }
        }

        for(Set<Integer> groupMembers : groupIdToUserIds.values()) {
            for(int userId : groupMembers) {
                for(int otherUserId : groupMembers) {
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

    public static Map<Integer, Set<Integer>> getInitialReplicasObeyingKReplication(int minNumReplicas, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> replicas = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            replicas.put(pid, new HashSet<Integer>());
        }

        Map<Integer, Set<Integer>> replicaLocations = new HashMap<>();
        for(Integer uid : friendships.keySet()) {
            replicaLocations.put(uid, new HashSet<Integer>());
        }

        Map<Integer, Integer> uMap = InitUtils.getUToMasterMap(partitions);

        //Step 1: add replicas for friends in different partitions
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                Integer pid1 = uMap.get(uid1);
                Integer pid2 = uMap.get(uid2);
                if(!pid1.equals(pid2)) {
                    replicas.get(pid1).add(uid2);
                    replicas.get(pid2).add(uid1);
                    replicaLocations.get(uid1).add(pid2);
                    replicaLocations.get(uid2).add(pid1);
                }
            }
        }

        //Step 2: add replicas as necessary for k-replication
        for(Integer uid : replicaLocations.keySet()) {
            int numShort = minNumReplicas - replicaLocations.get(uid).size();
            if(numShort > 0) {
                Set<Integer> possibilities = new HashSet<>(partitions.keySet());
                possibilities.removeAll(replicaLocations.get(uid));
                possibilities.remove(uMap.get(uid));
                Set<Integer> newReplicas = getKDistinctValuesFromList(numShort, possibilities);
                for(Integer pid : newReplicas) {
                    replicas.get(pid).add(uid);
                }
            }
        }

        return replicas;
    }

    public static Map<Integer, Set<Integer>> getRandomPartitioning(Set<Integer> pids, Set<Integer> uids) {
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        Set<Integer> pidCopies = new HashSet<>(pids);
        int numPartitions = pidCopies.size();
        int numUsers = uids.size();
        int usersPerPartition = numUsers / numPartitions;
        int numRemainderUsers = numUsers % numPartitions;
        Set<Integer> remainingUids = new HashSet<>(uids);
        while(remainingUids.size() > 0) {
            int k = usersPerPartition;
            if(numRemainderUsers > 0) {
                k++;
                numRemainderUsers--;
            }
            Set<Integer> pUids = ProbabilityUtils.getKDistinctValuesFromList(k, remainingUids);
            remainingUids.removeAll(pUids);
            Integer pid = pidCopies.iterator().next();
            pidCopies.remove(pid);
            partitions.put(pid, pUids);
        }
        return partitions;
    }

    public static Map<Integer, Set<Integer>> extractFriendshipsFromFile(String filename) throws Exception {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        File file = new File(filename);
        Scanner scanner = new Scanner(file);
        Set<Integer> allUids = new HashSet<>();
        while(scanner.hasNextLine()) {
            String line = scanner.nextLine();
            Integer uid = extractIdFromLine(line);
            Set<Integer> friends = extractFriendsFromLine(line);
            if(uid != null) {
                allUids.add(uid);
                if(friends != null) {
                    friendships.put(uid, friends);
                    allUids.addAll(friends);
                }
            }
        }
        allUids.removeAll(friendships.keySet());
        for(Integer friendless : allUids) {
            friendships.put(friendless, new HashSet<Integer>());
        }
        return friendships;
    }

    static Integer extractIdFromLine(String line) {
        if(line == null || line.length() < 2 || line.indexOf(':') < 0) {
            return null;
        }
        return Integer.parseInt(line.substring(0, line.indexOf(':')));
    }

    static Set<Integer> extractFriendsFromLine(String line) {
        if(line == null || line.length() < 4 || line.indexOf(':') < 0) {
            return null;
        }
        String mainPart = line.substring(line.indexOf(':')+2);
        if(mainPart.isEmpty()) {
            return null;
        }
        String[] ids = mainPart.split(", ");
        Set<Integer> idSet = new HashSet<>();
        for(String id : ids) {
            idSet.add(Integer.parseInt(id));
        }
        return idSet;
    }

    public static NavigableMap<Integer, NavigableSet<Integer>> copyMapSetNavigable(Map<Integer, Set<Integer>> m) {
        NavigableMap<Integer, NavigableSet<Integer>> copy = new TreeMap<>();
        for(Integer key : m.keySet()) {
            copy.put(key, new TreeSet<>(m.get(key)));
        }
        return copy;
    }

    public static Map<Integer, Set<Integer>> copyMapSet(Map<Integer, Set<Integer>> m) {
        Map<Integer, Set<Integer>> copy = new HashMap<>();
        for(Integer key : m.keySet()) {
            copy.put(key, new HashSet<>(m.get(key)));
        }
        return copy;
    }

    public static Set<Integer> findKeysForUser(Map<Integer, Set<Integer>> m, int uid) {
        Set<Integer> keys = new HashSet<>();
        for(int key : m.keySet()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

    public static int[] getNonFriendship(Map<Integer, Set<Integer>> friendships) {
        Integer userWhoIsntFriendsWithEveryone = null;
        while(userWhoIsntFriendsWithEveryone == null) {
            int chosenOne = ProbabilityUtils.getRandomElement(friendships.keySet());
            if(friendships.get(chosenOne).size() < friendships.size() - 1) {
                userWhoIsntFriendsWithEveryone = chosenOne;
            }
        }
        Set<Integer> nonfriends = new HashSet<>(friendships.keySet());
        nonfriends.removeAll(friendships.get(userWhoIsntFriendsWithEveryone));
        int friend = ProbabilityUtils.getRandomElement(nonfriends);
        return new int[] {userWhoIsntFriendsWithEveryone, friend};
    }

    public static int[] getFriendship(Map<Integer, Set<Integer>> friendships) {
        Set<Integer> possibilities = new HashSet<>();
        for(int uid : friendships.keySet()) {
            if(!friendships.get(uid).isEmpty()) {
                possibilities.add(uid);
            }
        }

        int chosenOne = ProbabilityUtils.getRandomElement(possibilities);
        return new int[]{chosenOne, ProbabilityUtils.getRandomElement(friendships.get(chosenOne))};
    }
}
