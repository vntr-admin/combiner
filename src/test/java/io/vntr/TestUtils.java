package io.vntr;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
}
