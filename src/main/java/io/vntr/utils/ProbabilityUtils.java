package io.vntr.utils;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.util.*;

public class ProbabilityUtils
{
	public static Set<Integer> getKDistinctValuesBetweenMandNInclusive(int k, int m, int n)
	{
		List<Integer> tempList = new LinkedList<Integer>();
		for(int i = m; i <= n; i++)
		{
			tempList.add(i);
		}

		return getKDistinctValuesFromList(k, tempList);
	}

	public static int getRandomElement(Collection<Integer> set) {
        return getKDistinctValuesFromList(1, new LinkedList<Integer>(set)).iterator().next();
    }

	public static Set<Integer> getKDistinctValuesFromList(int k, Collection<Integer> set)
	{
		return getKDistinctValuesFromList(k, new LinkedList<Integer>(set));
	}

	public static Set<Integer> getKDistinctValuesFromList(int k, List<Integer> list)
	{
		List<Integer> tempList = new LinkedList<Integer>(list);
//		Collections.copy(tempList, list);
		Set<Integer> returnSet = new HashSet<Integer>();
		for(int i=0; i<k; i++)
		{
			int index = (int)(Math.random() * tempList.size());
			returnSet.add(tempList.get(index));
			tempList.remove(index);
		}

		return returnSet;
	}

    public static Map<Integer, Set<Integer>> generateBidirectionalFriendshipSet(Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> bidirectionalFriendshipSet = new HashMap<Integer, Set<Integer>>();
        for(Integer uid : friendships.keySet()) {
            bidirectionalFriendshipSet.put(uid, new HashSet<Integer>());
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                bidirectionalFriendshipSet.get(uid1).add(uid2);
                bidirectionalFriendshipSet.get(uid2).add(uid1);
            }
        }
        return bidirectionalFriendshipSet;
    }

	public static double calculateAssortivityCoefficient(Map<Integer, Set<Integer>> friendships) {
		//We calculate "the Pearson correlation coefficient of the degrees at either ends of an edge"
        //Newman, M. E. (2002). Assortative mixing in networks. Physical review letters, 89(20), 208701.
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        int edgeCount = 0;
        for(Set<Integer> friends : friendships.values()) {
            edgeCount += friends.size();
        }
        double[] x = new double[edgeCount];
        double[] y = new double[edgeCount];
        int i=0;
        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                x[i] = bidirectionalFriendships.get(uid1).size();
                y[i] = bidirectionalFriendships.get(uid2).size();
                i++;
            }
        }

        double corr = new PearsonsCorrelation().correlation(x, y);
        return corr;
	}

}
