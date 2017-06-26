package io.vntr.utils;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.manager.RepManager;

/**
 * Created by robertlindquist on 4/26/17.
 */
public class InitUtils {
    public static NoRepManager initNoRepManager(double logicalMigrationRatio, boolean placeNewUserRandomly, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships) {
        NoRepManager manager = new NoRepManager(logicalMigrationRatio, placeNewUserRandomly);
        for(short pid : partitions.keys()) {
            manager.addPartition(pid);
            for(TShortIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
                manager.addUser(new User(iter.next(), pid));
            }
        }
        for (short uid1 : friendships.keys()) {
            for(TShortIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                manager.befriend(uid1, iter.next());
            }
        }
        return manager;
    }

    public static RepManager initRepManager(short minNumReplicas, double logicalMigrationRatio, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> replicaPartitions) {
        RepManager manager = new RepManager(minNumReplicas, logicalMigrationRatio);
        for(short pid : partitions.keys()) {
            manager.addPartition(pid);
        }

        TShortShortMap uToMasterMap = TroveUtils.getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uToReplicasMap = TroveUtils.getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(short uid : friendships.keys()) {
            short pid = uToMasterMap.get(uid);
            RepUser user = new RepUser(uid, pid);
            manager.addUser(user, pid);
        }

        for(short uid : friendships.keys()) {
            for(TShortIterator iter = uToReplicasMap.get(uid).iterator(); iter.hasNext(); ) {
                manager.addReplica(manager.getUserMaster(uid), iter.next());
            }
        }

        for(short uid : friendships.keys()) {
            for(TShortIterator iter = friendships.get(uid).iterator(); iter.hasNext(); ) {
                manager.befriend(manager.getUserMaster(uid), manager.getUserMaster(iter.next()));
            }
        }

        return manager;
    }
}
