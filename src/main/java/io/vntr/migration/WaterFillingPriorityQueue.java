package io.vntr.migration;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.NavigableSet;
import java.util.TreeSet;

import static io.vntr.utils.TroveUtils.getUserCounts;
import static io.vntr.utils.TroveUtils.invertTIntIntMap;

/**
 * Created by robertlindquist on 6/27/17.
 */
public class WaterFillingPriorityQueue {
    private final NavigableSet<Element> priorities;

    public WaterFillingPriorityQueue(TIntObjectMap<TIntSet> partitions, TIntIntMap strategy, int pidToSkip) {
        TIntIntMap pidToMasterCounts = getUserCounts(partitions);
        pidToMasterCounts.remove(pidToSkip);
        TIntObjectMap<TIntSet> invertedStrategy = invertTIntIntMap(strategy);
        for(int pid : partitions.keys()) {
            if(!invertedStrategy.containsKey(pid)) {
                invertedStrategy.put(pid, new TIntHashSet());
            }
        }
        invertedStrategy.remove(pidToSkip);
        priorities = new TreeSet<>();
        for(int pid : pidToMasterCounts.keys()) {
            int numMasters = pidToMasterCounts.get(pid) + invertedStrategy.get(pid).size();
            priorities.add(new Element(pid, numMasters));
        }
    }

    public int getNextPid() {
        Element next = priorities.iterator().next();
        next.numUsers++;
        return next.pid;
    }

    private static class Element implements Comparable<Element> {
        private int pid;
        private int numUsers;

        public Element(int pid, int numUsers) {
            this.pid = pid;
            this.numUsers = numUsers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Element)) return false;

            Element element = (Element) o;
            return pid == element.pid && numUsers == element.numUsers;
        }

        @Override
        public int hashCode() {
            return 31 * pid + numUsers;
        }

        @Override
        public int compareTo(Element o) {
            int userDelta = o.numUsers - this.numUsers;
            if(userDelta != 0) {
                return userDelta;
            }
            return this.pid - o.pid;
        }
    }
}
