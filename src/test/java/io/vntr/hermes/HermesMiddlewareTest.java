package io.vntr.hermes;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesMiddlewareTest {

    @Test
    public void testRemovePartition() {
        double gamma = 1.5;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        HermesManager manager = HermesTestUtils.initGraph(gamma, partitions, friendships);
//        assertTrue(manager.getEdgeCut() == 27); //or whatever
        //TODO: do this
    }

}
