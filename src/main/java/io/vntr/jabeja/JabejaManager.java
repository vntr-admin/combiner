package io.vntr.jabeja;

import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaManager {
    private Map<Long, JabejaUser> uMap;


    public JabejaManager() {
        uMap = new HashMap<Long, JabejaUser>();
    }

    public Long getPartitionForUser(Long uid) {
        return uMap.get(uid).getPid();
    }

    public Collection<JabejaUser> getRandomSamplingOfUsers(int n) {
        Set<JabejaUser> users = new HashSet<JabejaUser>();
        Set<Long> ids = ProbabilityUtils.getKDistinctValuesFromList(n, new LinkedList<Long>(uMap.keySet()));
        for(Long id : ids) {
            users.add(uMap.get(id));
        }
        return users;
    }
}
