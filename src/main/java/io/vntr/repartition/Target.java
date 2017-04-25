package io.vntr.repartition;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class Target implements Comparable<Target> {
    public final Integer uid;
    public final Integer pid;
    public final Integer oldPid;
    public final Float gain;

    public Target(Integer uid, Integer pid, Integer oldPid, Float gain) {
        this.uid = uid;
        this.pid = pid;
        this.oldPid = oldPid;
        this.gain = gain;
    }

    @Override
    public int compareTo(Target o) {
        int gainCompare = gain.compareTo(o.gain);
        if(gainCompare != 0) {
            return gainCompare;
        }
        int partCompare = pid.compareTo(o.pid);
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
        if (!pid.equals(target.pid)) return false;
        if (!oldPid.equals(target.oldPid)) return false;
        return gain.equals(target.gain);

    }

    @Override
    public int hashCode() {
        int result = uid.hashCode();
        result = 31 * result + pid.hashCode();
        result = 31 * result + oldPid.hashCode();
        result = 31 * result + gain.hashCode();
        return result;
    }

    @Override
    public String toString() {
        String strGain = gain > 0 ? "+" + gain : "" + gain;
        return uid + ": " + oldPid + "--(" + strGain + ")-->" + pid;
    }
}

