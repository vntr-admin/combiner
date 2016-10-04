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
        Set<T> set = new HashSet<T>();
        for(T t : args) {
            set.add(t);
        }
        return set;
    }

    public static <T> Set<T> initSet(Set<T> initialSet, T... args) {
        Set<T> set = new HashSet<T>();
        set.addAll(initialSet);
        for(T t : args) {
            set.add(t);
        }
        return set;
    }

    public static Map<Long, Set<Long>> getTopographyForMultigroupSocialNetwork(int numUsers, int numGroups, double groupMembershipProbability, double intraGroupFriendshipProbability) {
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

    public static Map<Long, Set<Long>> getInitialReplicasObeyingKReplication(int minNumReplicas, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> friendships) {
        Map<Long, Set<Long>> replicas = new HashMap<Long, Set<Long>>();
        for(Long pid : partitions.keySet()) {
            replicas.put(pid, new HashSet<Long>());
        }

        Map<Long, Set<Long>> replicaLocations = new HashMap<Long, Set<Long>>();
        for(Long uid : friendships.keySet()) {
            replicaLocations.put(uid, new HashSet<Long>());
        }

        Map<Long, Long> uMap = new HashMap<Long, Long>();
        for(Long pid : partitions.keySet()) {
            for(Long uid : partitions.get(pid)) {
                uMap.put(uid, pid);
            }
        }

        //Step 1: add replicas for friends in different partitions
        for(Long uid1 : friendships.keySet()) {
            for(Long uid2 : friendships.get(uid1)) {
                Long pid1 = uMap.get(uid1);
                Long pid2 = uMap.get(uid2);
                if(!pid1.equals(pid2)) {
                    replicas.get(pid1).add(uid2);
                    replicas.get(pid2).add(uid1);
                    replicaLocations.get(uid1).add(pid2);
                    replicaLocations.get(uid2).add(pid1);
                }
            }
        }

        //Step 2: add replicas as necessary for k-replication
        for(Long uid : replicaLocations.keySet()) {
            int numShort = minNumReplicas - replicaLocations.get(uid).size();
            if(numShort > 0) {
                Set<Long> possibilities = new HashSet<Long>(partitions.keySet());
                possibilities.removeAll(replicaLocations.get(uid));
                possibilities.remove(uMap.get(uid));
                Set<Long> newReplicas = getKDistinctValuesFromList(numShort, possibilities);
                for(Long pid : newReplicas) {
                    replicas.get(pid).add(uid);
                }
            }
        }

        return replicas;
    }

    public static Map<Long, Set<Long>> getRandomPartitioning(Set<Long> pids, Set<Long> uids) {
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        int numPartitions = pids.size();
        int numUsers = uids.size();
        int usersPerPartition = numUsers / numPartitions;
        int numRemainderUsers = numUsers % numPartitions;
        Set<Long> remainingUids = new HashSet<Long>(uids);
        while(remainingUids.size() > 0) {
            int k = usersPerPartition;
            if(numRemainderUsers > 0) {
                k++;
                numRemainderUsers--;
            }
            Set<Long> pUids = ProbabilityUtils.getKDistinctValuesFromList(k, remainingUids);
            remainingUids.removeAll(pUids);
            Long pid = pids.iterator().next();
            pids.remove(pid);
            partitions.put(pid, pUids);
        }
        return partitions;
    }

    public static Map<Long, Set<Long>> extractFriendshipsFromFile(String filename) throws Exception {
        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        File file = new File(filename);
        Scanner scanner = new Scanner(file);
        while(scanner.hasNextLine()) {
            String line = scanner.nextLine();
            Long uid = extractIdFromLine(line);
            Set<Long> friends = extractFriendsFromLine(line);
            if(uid != null && friends != null) {
                friendships.put(uid, friends);
            }
        }
        return friendships;
    }

    static Long extractIdFromLine(String line) {
        if(line == null || line.length() < 2 || line.indexOf(':') < 0) {
            return null;
        }
        return Long.parseLong(line.substring(0, line.indexOf(':')));
    }

    static Set<Long> extractFriendsFromLine(String line) {
        if(line == null || line.length() < 4 || line.indexOf(':') < 0) {
            return null;
        }
        String mainPart = line.substring(line.indexOf(':')+2);
        String[] ids = mainPart.split(", ");
        Set<Long> idSet = new HashSet<Long>();
        for(String id : ids) {
            idSet.add(Long.parseLong(id));
        }
        return idSet;
    }

}
