package io.vntr.hermes;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class Target implements Comparable<Target> {
    public final Long userId;
    public final Long partitionId;
    public final Long oldPartitionId;
    public final Integer gain;

    public Target(Long userId, Long partitionId, Long oldPartitionId, Integer gain) {
        this.userId = userId;
        this.partitionId = partitionId;
        this.oldPartitionId = oldPartitionId;
        this.gain = gain;
    }

    @Override
    public int compareTo(Target o) {
        int gainCompare = gain.compareTo(o.gain);
        if(gainCompare != 0) {
            return gainCompare;
        }
        int partCompare = partitionId.compareTo(o.partitionId);
        if(partCompare != 0) {
            return partCompare;
        }
        int oldPartCompare = oldPartitionId.compareTo(o.oldPartitionId);
        if(oldPartCompare != 0) {
            return oldPartCompare;
        }
        return userId.compareTo(o.userId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Target target = (Target) o;

        if (userId != null ? !userId.equals(target.userId) : target.userId != null) return false;
        if (partitionId != null ? !partitionId.equals(target.partitionId) : target.partitionId != null) return false;
        if (oldPartitionId != null ? !oldPartitionId.equals(target.oldPartitionId) : target.oldPartitionId != null)
            return false;
        return gain != null ? gain.equals(target.gain) : target.gain == null;

    }

    @Override
    public int hashCode() {
        int result = userId != null ? userId.hashCode() : 0;
        result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
        result = 31 * result + (oldPartitionId != null ? oldPartitionId.hashCode() : 0);
        result = 31 * result + (gain != null ? gain.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String strGain = gain > 0 ? "+" + gain : "" + gain;
        return userId + ": " + oldPartitionId + "--(" + strGain + ")-->" + partitionId;
    }
}
