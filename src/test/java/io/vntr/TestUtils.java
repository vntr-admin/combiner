package io.vntr;

 import gnu.trove.iterator.TShortIterator;
 import gnu.trove.map.TShortObjectMap;
 import gnu.trove.map.hash.TShortObjectHashMap;
 import gnu.trove.set.TShortSet;
 import gnu.trove.set.hash.TShortHashSet;
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

    public static TShortObjectMap<TShortSet> getTopographyForMultigroupSocialNetwork(int numUsers, int numGroups, float groupMembershipProbability, float intraGroupFriendshipProbability) {
        TShortObjectMap<TShortSet> userIdToFriendIds = new TShortObjectHashMap<>(numUsers+1);
        for(short id=0; id<numUsers; id++) {
            userIdToFriendIds.put(id, new TShortHashSet());
        }

        TShortObjectMap<TShortSet> groupIdToUserIds = new TShortObjectHashMap<>(numGroups+1);
        for(short id=0; id<numGroups; id++) {
            groupIdToUserIds.put(id, new TShortHashSet());
        }

        for(short userId : userIdToFriendIds.keys()) {
            for(short groupId : groupIdToUserIds.keys()) {
                if(Math.random() < groupMembershipProbability) {
                    groupIdToUserIds.get(groupId).add(userId);
                }
            }
        }

        for(TShortSet groupMembers : groupIdToUserIds.valueCollection()) {
            for(TShortIterator iter = groupMembers.iterator(); iter.hasNext(); ) {
                short uid = iter.next();
                for(TShortIterator iter2 = groupMembers.iterator(); iter2.hasNext(); ) {
                    short otherUid = iter2.next();
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

    public static TShortObjectMap<TShortSet> getRandomPartitioning(TShortSet pids, TShortSet uids) {
        TShortObjectMap<TShortSet> partitions = new TShortObjectHashMap<>(pids.size()+1);
        TShortSet pidCopies = new TShortHashSet(pids);
        int numPartitions = pidCopies.size();
        int numUsers = uids.size();
        short usersPerPartition = (short) (numUsers / numPartitions);
        int numRemainderUsers = numUsers % numPartitions;
        TShortSet remainingUids = new TShortHashSet(uids);
        while(remainingUids.size() > 0) {
            short k = usersPerPartition;
            if(numRemainderUsers > 0) {
                k++;
                numRemainderUsers--;
            }
            TShortSet pUids = TroveUtils.getKDistinctValuesFromArray(k, remainingUids.toArray());
            remainingUids.removeAll(pUids);
            short pid = pidCopies.iterator().next();
            pidCopies.remove(pid);
            partitions.put(pid, pUids);
        }
        return partitions;
    }

    public static TShortObjectMap<TShortSet> extractFriendshipsFromFile(String filename) throws Exception {
        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        File file = new File(filename);
        Scanner scanner = new Scanner(file);
        TShortSet allUids = new TShortHashSet();
        while(scanner.hasNextLine()) {
            String line = scanner.nextLine();
            Short uid = extractIdFromLine(line);
            TShortSet friends = extractFriendsFromLine(line);
            if(uid != null) {
                allUids.add(uid);
                if(friends != null) {
                    friendships.put(uid, friends);
                    allUids.addAll(friends);
                }
            }
        }
        allUids.removeAll(friendships.keySet());
        for(TShortIterator iter = allUids.iterator(); iter.hasNext(); ) {
            friendships.put(iter.next(), new TShortHashSet());
        }
        return friendships;
    }

    static Short extractIdFromLine(String line) {
        if(line == null || line.length() < 2 || line.indexOf(':') < 0) {
            return null;
        }
        return Short.parseShort(line.substring(0, line.indexOf(':')));
    }

    static TShortSet extractFriendsFromLine(String line) {
        if(line == null || line.length() < 4 || line.indexOf(':') < 0) {
            return null;
        }
        String mainPart = line.substring(line.indexOf(':')+2);
        if(mainPart.isEmpty()) {
            return null;
        }
        String[] ids = mainPart.split(", ");
        TShortSet idSet = new TShortHashSet(ids.length+1);
        for(String id : ids) {
            idSet.add(Short.parseShort(id));
        }
        return idSet;
    }

    public static TShortSet findKeysForUser(TShortObjectMap<TShortSet> m, short uid) {
        TShortSet keys = new TShortHashSet();
        for(short key : m.keys()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

}
