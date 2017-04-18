package io.vntr;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class Utils {
    public static boolean safeEquals(Object o1, Object o2) {
        if(o1 == o2) {
            return true;
        }
        if(o1 == null || o2 == null) {
            return false;
        }
        return o1.equals(o2);
    }

    public static int safeHashCode(Object o) {
        return o != null ? o.hashCode() : 1;
    }
}
