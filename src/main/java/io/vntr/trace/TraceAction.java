package io.vntr.trace;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class TraceAction {

    public enum ACTION {
        ADD_USER("+U"), REMOVE_USER("-U"), BEFRIEND("+F"), UNFRIEND("-F"), ADD_PARTITION("+P"), REMOVE_PARTITION("-P"), DOWNTIME("DT");

        private final String abbreviation;

        ACTION(String abbreviation) {
            this.abbreviation = abbreviation;
        }

        public String getAbbreviation() {
            return abbreviation;
        }

        public static ACTION fromName(String name) {
            for(ACTION TRACEAction : ACTION.values()) {
                if(TRACEAction.toString().equals(name)) {
                    return TRACEAction;
                }
            }

            return null;
        }
    }

    private final ACTION action;
    private short val1 = -1;
    private short val2 = -1;

    public TraceAction(ACTION action, short val1, short val2) {
        this.action = action;
        this.val1 = val1;
        this.val2 = val2;
    }

    public ACTION getAction() {
        return action;
    }

    public short getVal1() {
        return val1;
    }

    public short getVal2() {
        return val2;
    }

    @Override
    public String toString() {
        return action + " " + val1 + " " + val2;
    }

    public String toAbbreviatedString() { return action.getAbbreviation() + " " + val1 + " " + val2; }

    public static TraceAction fromString(String str) {
        try {
            String[] chunks = str.split(" ");
            ACTION TRACEAction = ACTION.fromName(chunks[0]);
            short val1 = Short.parseShort(chunks[1].trim());
            short val2 = Short.parseShort(chunks[2].trim());
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
        return action == that.action;
    }

    @Override
    public int hashCode() {
        int result = action != null ? action.hashCode() : 0;
        result = 31 * result + val1;
        result = 31 * result + val2;
        return result;
    }
}
