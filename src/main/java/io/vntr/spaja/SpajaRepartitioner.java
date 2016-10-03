package io.vntr.spaja;

import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaRepartitioner {
    private SpajaManager manager;
    private SpajaBefriendingStrategy spajaBefriendingStrategy;
    private int k;
    private double alpha;
    private double initialT;
    private double deltaT;

    public SpajaRepartitioner(SpajaManager manager, SpajaBefriendingStrategy spajaBefriendingStrategy) {
        this.k = manager.getMinNumReplicas();
        this.alpha = manager.getAlpha();
        this.initialT = manager.getInitialT();
        this.deltaT = manager.getDeltaT();
        this.manager = manager;
        this.spajaBefriendingStrategy = spajaBefriendingStrategy;
    }

    public void repartition() {
        for(double t = initialT; t >= 1; t -= deltaT) {
            List<Long> randomUserList = new LinkedList<Long>(manager.getAllUserIds());
            Collections.shuffle(randomUserList);
            for(Long uid : randomUserList) {
                SpajaUser user = manager.getUserMasterById(uid);
                SpajaUser partner = user.findPartner(manager.getUserMastersById(user.getFriendIDs()), t, spajaBefriendingStrategy);
                if(partner == null) {
                    partner = user.findPartner(getRandomSamplingOfUsers(k), t, spajaBefriendingStrategy);
                }
                if(partner != null) {
                    manager.swap(user.getId(), partner.getId(), spajaBefriendingStrategy);
                }
            }
        }
    }

    public Collection<SpajaUser> getRandomSamplingOfUsers(int n) {
        Set<Long> ids = getKDistinctValuesFromList(n, manager.getAllUserIds());
        return manager.getUserMastersById(ids);
    }
}
