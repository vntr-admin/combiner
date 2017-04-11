package io.vntr.hermar;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class Target implements Comparable<Target> {
    public final Integer userId;
    public final Integer partitionId;
    public final Integer oldPartitionId;
    public final Integer gain;

    public Target(Integer userId, Integer partitionId, Integer oldPartitionId, Integer gain) {
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

        if (!userId.equals(target.userId)) return false;
        if (!partitionId.equals(target.partitionId)) return false;
        if (!oldPartitionId.equals(target.oldPartitionId)) return false;
        return gain.equals(target.gain);

    }

    @Override
    public int hashCode() {
        int result = userId.hashCode();
        result = 31 * result + partitionId.hashCode();
        result = 31 * result + oldPartitionId.hashCode();
        result = 31 * result + gain.hashCode();
        return result;
    }

    @Override
    public String toString() {
        String strGain = gain > 0 ? "+" + gain : "" + gain;
        return userId + ": " + oldPartitionId + "--(" + strGain + ")-->" + partitionId;
    }
}
