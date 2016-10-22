package io.vntr.trace;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class FullTraceAction {
    private TRACE_ACTION TRACEAction;
    private int val1 = -1;
    private int val2 = -1;

    public FullTraceAction() {
    }

    public FullTraceAction(TRACE_ACTION TRACEAction) {
        this.TRACEAction = TRACEAction;
    }

    public FullTraceAction(TRACE_ACTION TRACEAction, int val1) {
        this.TRACEAction = TRACEAction;
        this.val1 = val1;
    }

    public FullTraceAction(TRACE_ACTION TRACEAction, int val1, int val2) {
        this.TRACEAction = TRACEAction;
        this.val1 = val1;
        this.val2 = val2;
    }

    public TRACE_ACTION getTRACEAction() {
        return TRACEAction;
    }

    public void setTRACEAction(TRACE_ACTION TRACEAction) {
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

    public static FullTraceAction fromString(String str) {
        try {
            String[] chunks = str.split(" ");
            TRACE_ACTION TRACEAction = TRACE_ACTION.fromName(chunks[0]);
            int val1 = Integer.parseInt(chunks[1].trim());
            int val2 = Integer.parseInt(chunks[2].trim());
            return new FullTraceAction(TRACEAction, val1, val2);
        } catch(Exception e) {
            return null;
        }
    }
}
