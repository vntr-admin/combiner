package io.vntr;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by robertlindquist on 9/24/16.
 */
public class TestUtils {
    public static <T> Set<T> initSet(T... args) {
        Set<T> set = new HashSet<T>();
        for(T t : args) {
            set.add(t);
        }
        return set;
    }
}
