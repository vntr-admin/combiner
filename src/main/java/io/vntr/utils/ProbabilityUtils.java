package io.vntr.utils;

import java.util.*;

public class ProbabilityUtils
{
	public static Set<Long> getKDistinctValuesBetweenMandNInclusive(long k, long m, long n)
	{
		List<Long> tempList = new LinkedList<Long>();
		for(long i = m; i <= n; i++)
		{
			tempList.add(i);
		}

		return getKDistinctValuesFromList(k, tempList);
	}

	public static Set<Long> getKDistinctValuesFromList(long k, Collection<Long> set)
	{
		return getKDistinctValuesFromList(k, new LinkedList<Long>(set));
	}

	public static Set<Long> getKDistinctValuesFromList(long k, List<Long> list)
	{
		List<Long> tempList = new LinkedList<Long>(list);
//		Collections.copy(tempList, list);
		Set<Long> returnSet = new HashSet<Long>();
		for(int i=0; i<k; i++)
		{
			int index = (int)(Math.random() * tempList.size());
			returnSet.add(tempList.get(index));
			tempList.remove(index);
		}

		return returnSet;
	}

}