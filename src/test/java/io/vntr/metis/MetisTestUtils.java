package io.vntr.metis;

import io.vntr.User;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 12/6/16.
 */
public class MetisTestUtils {

    private static final String temp_dir = "/Users/robertlindquist/Desktop/metistemp/";
    private static final String command_name = "/Users/robertlindquist/Downloads/metis-5.1.0/build/Darwin-x86_64/programs/gpmetis";

    public static MetisManager initGraph(Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        MetisManager manager = new MetisManager(command_name, temp_dir);
        for(Integer pid : partitions.keySet()) {
            manager.addPartition(pid);
            for(Integer uid : partitions.get(pid)) {
                manager.addUser(new User(uid, pid));
            }
        }
        for (Integer uid1 : friendships.keySet()) {
            for (Integer uid2 : friendships.get(uid1)) {
                manager.befriend(uid1, uid2);
            }
        }
        return manager;
    }
}
