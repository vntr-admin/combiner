package io.vntr.utils;

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

}