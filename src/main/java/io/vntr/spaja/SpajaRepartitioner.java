package io.vntr.spaja;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaRepartitioner {
    private SpajaManager manager;
    private SpajaBefriendingStrategy spajaBefriendingStrategy;
    private int k;
    private int randomSampingSize;
    private float alpha;
    private float initialT;
    private float deltaT;

    public SpajaRepartitioner(SpajaManager manager, SpajaBefriendingStrategy spajaBefriendingStrategy) {
        this.k = manager.getMinNumReplicas();
        this.alpha = manager.getAlpha();
        this.initialT = manager.getInitialT();
        this.deltaT = manager.getDeltaT();
        this.manager = manager;
        this.spajaBefriendingStrategy = spajaBefriendingStrategy;
        this.randomSampingSize = manager.getRandomSampingSize();
    }

    public void repartition() {
        for(float t = initialT; t >= 1; t -= deltaT) {
            List<Integer> randomUserList = new LinkedList<Integer>(manager.getAllUserIds());
            Collections.shuffle(randomUserList);
            for(Integer uid : randomUserList) {
                SpajaUser user = manager.getUserMasterById(uid);
                SpajaUser partner = user.findPartner(manager.getUserMastersById(user.getFriendIDs()), t, spajaBefriendingStrategy);
                if(partner == null) {
                    partner = user.findPartner(getRandomSamplingOfUsers(randomSampingSize), t, spajaBefriendingStrategy);
                }
                if(partner != null) {
                    manager.swap(user.getId(), partner.getId(), spajaBefriendingStrategy);
                }
            }
        }
    }

    public Collection<SpajaUser> getRandomSamplingOfUsers(int n) {
        Set<Integer> ids = getKDistinctValuesFromList(n, manager.getAllUserIds());
        return manager.getUserMastersById(ids);
    }
}
