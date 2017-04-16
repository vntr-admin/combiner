package io.vntr.spj2;

import io.vntr.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class SpJ2User extends User {

    private Integer masterPid;
    private Set<Integer> replicaPids;

    public SpJ2User(Integer id) {
        super(id);
        replicaPids = new HashSet<>();
    }

    public Integer getMasterPid() {
        return masterPid;
    }

    public void setMasterPid(Integer masterPid) {
        this.masterPid = masterPid;
    }

    public void addReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.add(replicaPartitionId);
    }

    public void removeReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.remove(replicaPartitionId);
    }

    public void addReplicaPartitionIds(Collection<Integer> replicaPartitionIds) {
        this.replicaPids.addAll(replicaPartitionIds);
    }

    public Set<Integer> getReplicaPids() {
        return replicaPids;
    }

    @Override
    public SpJ2User clone() {
        SpJ2User user = new SpJ2User(getId());
        user.setMasterPid(masterPid);
        user.addReplicaPartitionIds(replicaPids);
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    @Override
    public String toString() {
        return super.toString() + "|masterP:" + masterPid + "|Reps:" + replicaPids.toString();
    }
}
