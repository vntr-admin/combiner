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

	public static double calculateAssortivity(Map<Integer, Set<Integer>> friendships) {
		Map<Integer, Integer> nodeDegree = new HashMap<Integer, Integer>();
		Map<Integer, Double> sumOfFriendsDegrees = new HashMap<Integer, Double>();
		for(int uid : friendships.keySet()) {
			nodeDegree.put(uid, 0);
			sumOfFriendsDegrees.put(uid, 0D);
		}
		for(int uid1 : friendships.keySet()) {
			nodeDegree.put(uid1, nodeDegree.get(uid1) + friendships.get(uid1).size());
			for(int uid2 : friendships.get(uid1)) {
				nodeDegree.put(uid2, nodeDegree.get(uid2) + 1);
			}
		}

		for(int uid1 : friendships.keySet()) {
			double degreeOfUid1 = nodeDegree.get(uid1);
			for(int uid2 : friendships.get(uid1)) {
				double degreeOfUid2 = nodeDegree.get(uid2);
				sumOfFriendsDegrees.put(uid1, sumOfFriendsDegrees.get(uid1) + degreeOfUid2);
				sumOfFriendsDegrees.put(uid2, sumOfFriendsDegrees.get(uid2) + degreeOfUid1);
			}
		}

		Map<Integer, Double> averageDegreeOfFriends = new HashMap<Integer, Double>();
		for(int uid : friendships.keySet()) {
		    double degree = (nodeDegree.get(uid) == 0) ? 1 : nodeDegree.get(uid);
			averageDegreeOfFriends.put(uid, sumOfFriendsDegrees.get(uid) / degree);
		}

		double[] x = new double[friendships.size()];
		double[] y = new double[friendships.size()];

		int index = 0;
		for(Iterator<Integer> iter = friendships.keySet().iterator(); iter.hasNext(); index++) {
			int uid = iter.next();
			double degree = nodeDegree.get(uid);
			double avgFriendDegree = averageDegreeOfFriends.get(uid);
			x[index] = degree;
			y[index] = avgFriendDegree;
		}

		double corr = new PearsonsCorrelation().correlation(y, x);
		return corr;
	}
}