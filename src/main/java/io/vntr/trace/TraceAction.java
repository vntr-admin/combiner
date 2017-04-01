package io.vntr.trace;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class TraceAction {

    public enum ACTION {
        ADD_USER, REMOVE_USER, BEFRIEND, UNFRIEND, ADD_PARTITION, REMOVE_PARTITION, DOWNTIME, FOREST_FIRE;

        public static ACTION fromName(String name) {
            for(ACTION TRACEAction : ACTION.values()) {
                if(TRACEAction.toString().equals(name)) {
                    return TRACEAction;
                }
            }

            return null;
        }
    }

    private ACTION TRACEAction;
    private int val1 = -1;
    private int val2 = -1;

    public TraceAction() {
    }

    public TraceAction(ACTION TRACEAction) {
        this.TRACEAction = TRACEAction;
    }

    public TraceAction(ACTION TRACEAction, int val1) {
        this.TRACEAction = TRACEAction;
        this.val1 = val1;
    }

    public TraceAction(ACTION TRACEAction, int val1, int val2) {
        this.TRACEAction = TRACEAction;
        this.val1 = val1;
        this.val2 = val2;
    }

    public ACTION getTRACEAction() {
        return TRACEAction;
    }

    public void setTRACEAction(ACTION TRACEAction) {
        this.TRACEAction = TRACEAction;
    }

    public int getVal1() {
        return val1;
    }

    public void setVal1(int val1) {
        this.val1 = val1;
    }

    public int getVal2() {
        return val2;
    }

    public void setVal2(int val2) {
        this.val2 = val2;
    }

    @Override
    public String toString() {
        return TRACEAction + " " + val1 + " " + val2;
    }

    public static TraceAction fromString(String str) {
        try {
            String[] chunks = str.split(" ");
            ACTION TRACEAction = ACTION.fromName(chunks[0]);
            int val1 = Integer.parseInt(chunks[1].trim());
            int val2 = Integer.parseInt(chunks[2].trim());
            return new TraceAction(TRACEAction, val1, val2);
        } catch(Exception e) {
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TraceAction that = (TraceAction) o;

        if (val1 != that.val1) return false;
        if (val2 != that.val2) return false;
        return TRACEAction == that.TRACEAction;
    }

    @Override
    public int hashCode() {
        int result = TRACEAction != null ? TRACEAction.hashCode() : 0;
        result = 31 * result + val1;
        result = 31 * result + val2;
        return result;
    }
}
