package io.vntr.jabeja;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class JabejaTestUtils {
    public static JabejaManager initGraph(double alpha, double initialT, double deltaT, int k, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> friendships) {
        JabejaManager manager = new JabejaManager(alpha, initialT, deltaT, k);
        for(Long pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Long uid : partitions.get(pid)) {
                manager.addUser(new JabejaUser("User " + uid, uid, pid, alpha, manager));
            }
        }
        for(Long uid1 : friendships.keySet()) {
            for(Long uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }

    public static List<JabejaUser> getUsers(JabejaManager manager, Long... uids) {
        List<JabejaUser> list = new LinkedList<JabejaUser>();
        for(Long uid : uids) {
            list.add(manager.getUser(uid));
        }
        return list;
    }

    public static <T> Set<T> initSet(T... args) {
        Set<T> set = new HashSet<T>();
        for(T t : args) {
            set.add(t);
        }
        return set;
    }
}
