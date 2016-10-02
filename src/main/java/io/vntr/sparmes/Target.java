package io.vntr.sparmes;

/**
 * Created by robertlindquist on 9/29/16.
 */
public class Target implements Comparable<Target> {
    public final Long uid;
    public final Long newPid;
    public final Long oldPid;
    public final Integer gain;

    public Target(Long uid, Long newPid, Long oldPid, Integer gain) {
        this.uid = uid;
        this.newPid = newPid;
        this.oldPid = oldPid;
        this.gain = gain;
    }

    @Override
    public int compareTo(Target o) {
        int gainCompare = gain.compareTo(o.gain);
        if(gainCompare != 0) {
            return gainCompare;
        }
        int partCompare = newPid.compareTo(o.newPid);
        if(partCompare != 0) {
            return partCompare;
        }
        int oldPartCompare = oldPid.compareTo(o.oldPid);
        if(oldPartCompare != 0) {
            return oldPartCompare;
        }
        return uid.compareTo(o.uid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Target target = (Target) o;

        if (!uid.equals(target.uid)) return false;
        if (!newPid.equals(target.newPid)) return false;
        if (!oldPid.equals(target.oldPid)) return false;
        return gain.equals(target.gain);

    }

    @Override
    public int hashCode() {
        int result = uid.hashCode();
        result = 31 * result + newPid.hashCode();
        result = 31 * result + oldPid.hashCode();
        result = 31 * result + gain.hashCode();
        return result;
    }

    @Override
    public String toString() {
        String strGain = gain > 0 ? "+" + gain : "" + gain;
        return uid + ": " + oldPid + "--(" + strGain + ")-->" + newPid;
    }
}

