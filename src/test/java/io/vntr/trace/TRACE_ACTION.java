package io.vntr.trace;

/**
 * Created by robertlindquist on 10/20/16.
 */
public enum TRACE_ACTION {
    ADD_USER, REMOVE_USER, BEFRIEND, UNFRIEND, ADD_PARTITION, REMOVE_PARTITION, DOWNTIME, FOREST_FIRE;

    public static TRACE_ACTION fromName(String name) {
        for(TRACE_ACTION TRACEAction : TRACE_ACTION.values()) {
            if(TRACEAction.toString().equals(name)) {
                return TRACEAction;
            }
        }

        return null;
    }
}
