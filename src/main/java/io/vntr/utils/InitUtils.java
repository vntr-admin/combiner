package io.vntr.utils;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.manager.RepManager;

/**
 * Created by robertlindquist on 4/26/17.
 */
public class InitUtils {
    public static NoRepManager initNoRepManager(double logicalMigrationRatio, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships) {
        NoRepManager manager = new NoRepManager(logicalMigrationRatio);
        for(Integer pid : partitions.keys()) {
            manager.addPartition(pid);
            for(TIntIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
                manager.addUser(new User(iter.next(), pid));
            }
        }
        for (Integer uid1 : friendships.keys()) {
            for(TIntIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                manager.befriend(uid1, iter.next());
            }
        }
        return manager;
    }

    public static RepManager initRepManager(int minNumReplicas, double logicalMigrationRatio, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> replicaPartitions) {
        RepManager manager = new RepManager(minNumReplicas, logicalMigrationRatio);
        for(Integer pid : partitions.keys()) {
            manager.addPartition(pid);
        }

        TIntIntMap uToMasterMap = TroveUtils.getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uToReplicasMap = TroveUtils.getUToReplicasMap(replicaPartitions, uToMasterMap.keySet());

        for(Integer uid : friendships.keys()) {
            Integer pid = uToMasterMap.get(uid);
            RepUser user = new RepUser(uid, pid);
            manager.addUser(user, pid);
        }

        for(Integer uid : friendships.keys()) {
            for(TIntIterator iter = uToReplicasMap.get(uid).iterator(); iter.hasNext(); ) {
                manager.addReplica(manager.getUserMaster(uid), iter.next());
            }
        }

        for(Integer uid : friendships.keys()) {
            for(TIntIterator iter = friendships.get(uid).iterator(); iter.hasNext(); ) {
                manager.befriend(manager.getUserMaster(uid), manager.getUserMaster(iter.next()));
            }
        }

        return manager;
    }
}
