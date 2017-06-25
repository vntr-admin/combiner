package io.vntr;

 import gnu.trove.iterator.TIntIterator;
 import gnu.trove.map.TIntObjectMap;
 import gnu.trove.map.hash.TIntObjectHashMap;
 import gnu.trove.set.TIntSet;
 import gnu.trove.set.hash.TIntHashSet;
 import io.vntr.utils.TroveUtils;

 import java.io.File;
 import java.util.*;


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

    public static TIntObjectMap<TIntSet> getTopographyForMultigroupSocialNetwork(int numUsers, int numGroups, float groupMembershipProbability, float intraGroupFriendshipProbability) {
        TIntObjectMap<TIntSet> userIdToFriendIds = new TIntObjectHashMap<>(numUsers+1);
        for(int id=0; id<numUsers; id++) {
            userIdToFriendIds.put(id, new TIntHashSet());
        }

        TIntObjectMap<TIntSet> groupIdToUserIds = new TIntObjectHashMap<>(numGroups+1);
        for(int id=0; id<numGroups; id++) {
            groupIdToUserIds.put(id, new TIntHashSet());
        }

        for(Integer userId : userIdToFriendIds.keys()) {
            for(Integer groupId : groupIdToUserIds.keys()) {
                if(Math.random() < groupMembershipProbability) {
                    groupIdToUserIds.get(groupId).add(userId);
                }
            }
        }

        for(TIntSet groupMembers : groupIdToUserIds.valueCollection()) {
            for(TIntIterator iter = groupMembers.iterator(); iter.hasNext(); ) {
                int uid = iter.next();
                for(TIntIterator iter2 = groupMembers.iterator(); iter2.hasNext(); ) {
                    int otherUid = iter2.next();
                    if(uid < otherUid) { //this avoids running it once for each user
                        if(Math.random() < intraGroupFriendshipProbability) {
                            userIdToFriendIds.get(uid).add(otherUid);
                            userIdToFriendIds.get(otherUid).add(uid);
                        }
                    }
                }
            }
        }

        return userIdToFriendIds;
    }

    public static TIntObjectMap<TIntSet> getRandomPartitioning(TIntSet pids, TIntSet uids) {
        TIntObjectMap<TIntSet> partitions = new TIntObjectHashMap<>(pids.size()+1);
        TIntSet pidCopies = new TIntHashSet(pids);
        int numPartitions = pidCopies.size();
        int numUsers = uids.size();
        int usersPerPartition = numUsers / numPartitions;
        int numRemainderUsers = numUsers % numPartitions;
        TIntSet remainingUids = new TIntHashSet(uids);
        while(remainingUids.size() > 0) {
            int k = usersPerPartition;
            if(numRemainderUsers > 0) {
                k++;
                numRemainderUsers--;
            }
            TIntSet pUids = TroveUtils.getKDistinctValuesFromArray(k, remainingUids.toArray());
            remainingUids.removeAll(pUids);
            Integer pid = pidCopies.iterator().next();
            pidCopies.remove(pid);
            partitions.put(pid, pUids);
        }
        return partitions;
    }

    public static TIntObjectMap<TIntSet> extractFriendshipsFromFile(String filename) throws Exception {
        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        File file = new File(filename);
        Scanner scanner = new Scanner(file);
        TIntSet allUids = new TIntHashSet();
        while(scanner.hasNextLine()) {
            String line = scanner.nextLine();
            Integer uid = extractIdFromLine(line);
            TIntSet friends = extractFriendsFromLine(line);
            if(uid != null) {
                allUids.add(uid);
                if(friends != null) {
                    friendships.put(uid, friends);
                    allUids.addAll(friends);
                }
            }
        }
        allUids.removeAll(friendships.keySet());
        for(TIntIterator iter = allUids.iterator(); iter.hasNext(); ) {
            friendships.put(iter.next(), new TIntHashSet());
        }
        return friendships;
    }

    static Integer extractIdFromLine(String line) {
        if(line == null || line.length() < 2 || line.indexOf(':') < 0) {
            return null;
        }
        return Integer.parseInt(line.substring(0, line.indexOf(':')));
    }

    static TIntSet extractFriendsFromLine(String line) {
        if(line == null || line.length() < 4 || line.indexOf(':') < 0) {
            return null;
        }
        String mainPart = line.substring(line.indexOf(':')+2);
        if(mainPart.isEmpty()) {
            return null;
        }
        String[] ids = mainPart.split(", ");
        TIntSet idSet = new TIntHashSet(ids.length+1);
        for(String id : ids) {
            idSet.add(Integer.parseInt(id));
        }
        return idSet;
    }

    public static TIntSet findKeysForUser(TIntObjectMap<TIntSet> m, int uid) {
        TIntSet keys = new TIntHashSet();
        for(int key : m.keys()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

}
