package io.vntr.spaja;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaRepartitioner {
    private SpajaManager manager;
    private SpajaBefriendingStrategy spajaBefriendingStrategy;
    private int k;
    private int randomSampingSize;
    private float alpha;
    private float initialT;
    private float deltaT;

    public SpajaRepartitioner(SpajaManager manager, SpajaBefriendingStrategy spajaBefriendingStrategy) {
        this.k = manager.getMinNumReplicas();
        this.alpha = manager.getAlpha();
        this.initialT = manager.getInitialT();
        this.deltaT = manager.getDeltaT();
        this.manager = manager;
        this.spajaBefriendingStrategy = spajaBefriendingStrategy;
        this.randomSampingSize = manager.getRandomSampingSize();
    }

    public void repartition() {
        for(float t = initialT; t >= 1; t -= deltaT) {
            List<Integer> randomUserList = new LinkedList<Integer>(manager.getAllUserIds());
            Collections.shuffle(randomUserList);
            for(Integer uid : randomUserList) {
                SpajaUser user = manager.getUserMasterById(uid);
                SpajaUser partner = user.findPartner(manager.getUserMastersById(user.getFriendIDs()), t, spajaBefriendingStrategy);
                if(partner == null) {
                    partner = user.findPartner(getRandomSamplingOfUsers(randomSampingSize), t, spajaBefriendingStrategy);
                }
                if(partner != null) {
//                    if(!isMiddlewareInAValidState(2)) {
//                        isMiddlewareInAValidState(2);
//                    }
                    String u1Str = user.toString();
                    String u2Str = partner.toString();
                    Map<Integer, String> u1Friends = new HashMap<Integer, String>();
                    for(int friendId : user.getFriendIDs()) {
                        u1Friends.put(friendId, manager.getUserMasterById(friendId).toString());
                    }
                    Map<Integer, String> u2Friends = new HashMap<Integer, String>();
                    for(int friendId : partner.getFriendIDs()) {
                        u2Friends.put(friendId, manager.getUserMasterById(friendId).toString());
                    }
                    manager.swap(user.getId(), partner.getId(), spajaBefriendingStrategy);
//                    if(!isMiddlewareInAValidState(2)) {
//                        isMiddlewareInAValidState(2);
//                    }
                }
            }
        }
    }

    private boolean isMiddlewareInAValidState(int minNumReplicas) {
        //TODO: add back in the replica-specific stuff
        boolean isValid = true;
        Set<Integer> pids = new HashSet<Integer>(manager.getAllPartitionIds());
        Set<Integer> uids = new HashSet<Integer>(manager.getAllUserIds());

        Map<Integer, Set<Integer>> partitions  = manager.getPartitionToUserMap();
        Map<Integer, Set<Integer>> replicas    = manager.getPartitionToReplicaMap();
        Map<Integer, Set<Integer>> friendships = manager.getFriendships();

        isValid &= (pids.equals(partitions.keySet()));
        isValid &= (pids.equals(replicas.keySet()));
        isValid &= (uids.equals(friendships.keySet()));

        for(int uid : uids) {
            int numMasters = findKeysForUser(partitions, uid).size();
            int numReplicas = findKeysForUser(replicas, uid).size();
            if(numMasters != 1 || numReplicas < minNumReplicas) {
                System.out.println("Blork");
            }
            isValid &= (findKeysForUser(partitions, uid).size() == 1);
            isValid &= (findKeysForUser(replicas, uid).size() >= minNumReplicas);
        }

        for(int uid : uids) {
            int pid = findKeysForUser(partitions, uid).iterator().next();
            Set<Integer> reps = findKeysForUser(replicas, uid);
            if(reps.contains(pid)) {
                System.out.println(uid + " has a master in " + pid + " and replicas in " + reps);
                isValid = false;
            }
        }

        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                int pid1 = findKeysForUser(partitions, uid1).iterator().next();
                int pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    boolean pid2HasUid1 = findKeysForUser(replicas, uid1).contains(pid2);
                    boolean pid1HasUid2 = findKeysForUser(replicas, uid2).contains(pid1);
                    if(!pid1HasUid2 || !pid2HasUid1) {
                        System.out.println("Blork");
                    }
                    //If they aren't colocated, they have replicas in each other's partitions
                    isValid &= (findKeysForUser(replicas, uid1).contains(pid2));
                    isValid &= (findKeysForUser(replicas, uid2).contains(pid1));
                }
            }
        }

        //Assert that replicas are consistent with the master in everything except partitionId
        for(int uid : friendships.keySet()) {
            SpajaUser master = manager.getUserMasterById(uid);
            Set<Integer> replicaPids = findKeysForUser(replicas, uid);
            for(int replicaPid : replicaPids) {
                SpajaUser replica = manager.getPartitionById(replicaPid).getReplicaById(uid);
                isValid &= (master.getId().equals(replica.getId()));
                isValid &= (master.getFriendIDs().equals(replica.getFriendIDs()));
                isValid &= (master.getMasterPartitionId().equals(replica.getMasterPartitionId()));
                isValid &= (master.getReplicaPartitionIds().equals(replica.getReplicaPartitionIds()));
                isValid &= (master.getPartitionId().equals(master.getMasterPartitionId()));
                isValid &= (replica.getPartitionId().equals((Integer) replicaPid));
            }
        }

        Set<Integer> allMastersSeen = new HashSet<Integer>();
        for(int pid : partitions.keySet()) {
            allMastersSeen.addAll(partitions.get(pid));
        }
        allMastersSeen.removeAll(manager.getAllUserIds());
        isValid &= (allMastersSeen.isEmpty());

        Set<Integer> allReplicasSeen = new HashSet<Integer>();
        for(int pid : replicas.keySet()) {
            allReplicasSeen.addAll(replicas.get(pid));
        }
        allReplicasSeen.removeAll(manager.getAllUserIds());
        isValid &= (allReplicasSeen.isEmpty());

        Set<Integer> allFriendsSeen = new HashSet<Integer>();
        for(int pid : friendships.keySet()) {
            allFriendsSeen.addAll(friendships.get(pid));
        }
        allFriendsSeen.removeAll(manager.getAllUserIds());
        isValid &= (allFriendsSeen.isEmpty());
        return isValid;
    }

    private static Set<Integer> findKeysForUser(Map<Integer, Set<Integer>> m, int uid) {
        Set<Integer> keys = new HashSet<Integer>();
        for(int key : m.keySet()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

    public Collection<SpajaUser> getRandomSamplingOfUsers(int n) {
        Set<Integer> ids = getKDistinctValuesFromList(n, manager.getAllUserIds());
        return manager.getUserMastersById(ids);
    }
}
