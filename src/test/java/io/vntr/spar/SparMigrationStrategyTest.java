package io.vntr.spar;

import io.vntr.User;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.*;

public class SparMigrationStrategyTest
{
	@Test
	public void testScoreReplicaPromotion()
	{
		SparManager manager = new SparManager(2);
		SparMigrationStrategy strategy = new SparMigrationStrategy(manager);

		manager.addPartition();
		manager.addPartition();
		manager.addPartition();
		manager.addPartition();
		manager.addPartition();

		Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
		Long userId1 = 23L;
		User user1 = new User("Anita", userId1);
		manager.addUser(user1);
		userIdToUserMap.put(userId1, user1);

		Long userId2 = 15L;
		User user2 = new User("Bob", userId2);
		manager.addUser(user2);
		userIdToUserMap.put(userId2, user2);

		Long userId3 = 2L;
		User user3 = new User("Carol", userId3);
		manager.addUser(user3);
		userIdToUserMap.put(userId3, user3);

		Long userId4 = 7L;
		User user4 = new User("Declan", userId4);
		manager.addUser(user4);
		userIdToUserMap.put(userId4, user4);

		Long userId5 = 9L;
		User user5 = new User("Erin", userId5);
		manager.addUser(user5);
		userIdToUserMap.put(userId5, user5);

		SparUser sparUser1 = manager.getUserMasterById(userId1);
		SparUser sparUser3 = manager.getUserMasterById(userId3);

		manager.befriend(sparUser3, sparUser1);

		for(Long partitionId : manager.getAllPartitionIds())
		{
			assertTrue(manager.getPartitionById(partitionId).getNumMasters() == 1);
		}

		Long userId6 = 29L;
		User user6 = new User("Francisco", userId6);
		manager.addUser(user6);
		userIdToUserMap.put(userId6, user6);

		SparUser sparUser6 = manager.getUserMasterById(userId6);

outer:	for(Long partitionId : sparUser6.getReplicaPartitionIds())
		{
			for(Long userId : userIdToUserMap.keySet())
			{
				SparUser sparUser = manager.getUserMasterById(userId);
				if(sparUser.getMasterPartitionId().equals(partitionId))
				{
					manager.befriend(sparUser, sparUser6);
					break outer;
				}
			}
		}

		Long friendPartitionId = manager.getUserMasterById(sparUser6.getFriendIDs().iterator().next()).getMasterPartitionId();

		assertTrue(strategy.scoreReplicaPromotion(sparUser6, friendPartitionId) == 1D);
		Set<Long> replicaPartitionIds = new HashSet<Long>(sparUser6.getReplicaPartitionIds());
		replicaPartitionIds.remove(friendPartitionId);
		Long nonFriendPartitionId = replicaPartitionIds.iterator().next();
		assertTrue(strategy.scoreReplicaPromotion(sparUser6, nonFriendPartitionId) == 0D);

		Long[]   userIdArray =   {    3L,          4L,       5L,      6L,       8L,         10L,    11L,   12L,      13L};
		String[] userNameArray = {"Greg", "Henrietta", "Imogen", "James", "Kartuk", "Llewellyn", "Ming", "Nao", "Ortega"};
		for(int i=0; i<9; i++)
		{
			User user = new User(userNameArray[i], userIdArray[i]);
			manager.addUser(user);
			userIdToUserMap.put(userIdArray[i], user);
		}

		Set<SparUser> usersOnNonFriendPartition = new HashSet<SparUser>();
		for(Long userId : userIdToUserMap.keySet())
		{
			SparUser sparUser = manager.getUserMasterById(userId);
			if(sparUser.getMasterPartitionId().equals(nonFriendPartitionId))
			{
				usersOnNonFriendPartition.add(sparUser);
			}
		}

		assertTrue(usersOnNonFriendPartition.size() == 3);

		for(SparUser sparUser : usersOnNonFriendPartition)
		{
			manager.befriend(sparUser, sparUser6);
		}

		assertTrue( (strategy.scoreReplicaPromotion(sparUser6, nonFriendPartitionId)) == 2.25);
	}

	@Test
	public void testGetRemainingSpotsInPartitions()
	{
		SparManager manager = new SparManager(2);
		SparMigrationStrategy strategy = new SparMigrationStrategy(manager);

		manager.addPartition();
		manager.addPartition();
		manager.addPartition();
		manager.addPartition();
		manager.addPartition();
		
		Map<Long, User> userIdToUserMap = new HashMap<Long, User>();
		Long[]   userIdArray =   {    3L,          4L,       5L,      6L,       8L,         10L,    11L,   12L};
		String[] userNameArray = {"Greg", "Henrietta", "Imogen", "James", "Kartuk", "Llewellyn", "Ming", "Nao"};
		for(int i=0; i<8; i++)
		{
			User user = new User(userNameArray[i], userIdArray[i]);
			manager.addUser(user);
			userIdToUserMap.put(userIdArray[i], user);
		}

		Set<Long> partitionsWithOnlyOneMaster = new HashSet<Long>();
		for(Long partitionId : manager.getAllPartitionIds())
		{
			if(manager.getPartitionById(partitionId).getNumMasters() == 1)
			{
				partitionsWithOnlyOneMaster.add(partitionId);
			}
		}

		Map<Long, Integer> remainingSpotsInPartitions = strategy.getRemainingSpotsInPartitions(new HashSet<Long>());
		for(Long partitionId : manager.getAllPartitionIds())
		{
			if(partitionsWithOnlyOneMaster.contains(partitionId))
			{
				assertTrue(remainingSpotsInPartitions.get(partitionId).intValue() == 1);
			}
			else
			{
				assertTrue(remainingSpotsInPartitions.get(partitionId).intValue() == 0);
			}
		}

		Long partitionIdToRob = partitionsWithOnlyOneMaster.iterator().next();
		Set<Long> partitionsWithTwoMasters = new HashSet<Long>(manager.getAllPartitionIds());
		partitionsWithTwoMasters.removeAll(partitionsWithOnlyOneMaster);
		Long partitionIdToSendTo = partitionsWithTwoMasters.iterator().next();

		SparUser user = manager.getUserMasterById(manager.getPartitionById(partitionIdToRob).getIdsOfMasters().iterator().next());
		manager.moveUser(user, partitionIdToSendTo, new HashSet<Long>(), new HashSet<Long>());

		Map<Long, Integer> remainingSpotsInPartitions2 = strategy.getRemainingSpotsInPartitions(new HashSet<Long>());

		for(Long partitionId : manager.getAllPartitionIds())
		{
			if(partitionIdToRob.equals(partitionId))
			{
				assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == 2);
			}
			else if(partitionIdToSendTo.equals(partitionId))
			{
				assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == -1);
			}
			else if(partitionsWithOnlyOneMaster.contains(partitionId))
			{
				assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == 1);
			}
			else if(partitionsWithTwoMasters.contains(partitionId))
			{
				assertTrue(remainingSpotsInPartitions2.get(partitionId).intValue() == 0);
			}
		}

		Long partitionIdWithTwoMasters = -1L;
		for(Long partitionId : manager.getAllPartitionIds())
		{
			if(manager.getPartitionById(partitionId).getNumMasters() == 2)
			{
				partitionIdWithTwoMasters = partitionId;
				break;
			}
		}

		Map<Long, Integer> remainingSpotsInPartitions3 = strategy.getRemainingSpotsInPartitions(new HashSet<Long>(Arrays.asList(partitionIdWithTwoMasters)));
		for(Long partitionId : manager.getAllPartitionIds())
		{
			if(!partitionId.equals(partitionIdWithTwoMasters))
			{
				int numMasters = manager.getPartitionById(partitionId).getNumMasters();
				int claimedRemainingSpots = remainingSpotsInPartitions3.get(partitionId);
				assertTrue(numMasters + claimedRemainingSpots == 2);
			}
		}
	}

	@Test
	public void testGetWaterFillingStrategyOfPartitions()
	{
		SparManager manager = TestUtils.getStandardManager();
		SparMigrationStrategy strategy = new SparMigrationStrategy(manager);

		Map<Long, Integer> partitionIdToNumMastersMap = new HashMap<Long, Integer>();
		for(Long partitionId : manager.getAllPartitionIds())
		{
			partitionIdToNumMastersMap.put(partitionId, manager.getPartitionById(partitionId).getNumMasters());
		}

		List<Long> waterFillingStrategy = strategy.getWaterFillingStrategyOfPartitions(new HashSet<Long>(), new HashMap<Long, Long>(), 7);
		Map<Long, Integer> partitionIdToNumMastersMapCopy = new HashMap<Long, Integer>(partitionIdToNumMastersMap);
		for(Long partitionId : waterFillingStrategy)
		{
			partitionIdToNumMastersMapCopy.put(partitionId, partitionIdToNumMastersMapCopy.get(partitionId) + 1);
		}

		for(Long partitionId : partitionIdToNumMastersMapCopy.keySet())
		{
			assertTrue(partitionIdToNumMastersMapCopy.get(partitionId).intValue() == 3);
		}

		Long partitionIdWithTwoMasters = null;
		for(Long partitionId : partitionIdToNumMastersMap.keySet())
		{
			if(partitionIdToNumMastersMap.get(partitionId).intValue() == 2)
			{
				partitionIdWithTwoMasters = partitionId;
				break;
			}
		}

		List<Long> secondWaterFillingStrategy = strategy.getWaterFillingStrategyOfPartitions(new HashSet<Long>(Arrays.asList(partitionIdWithTwoMasters)), new HashMap<Long, Long>(), 6);
		partitionIdToNumMastersMapCopy = new HashMap<Long, Integer>(partitionIdToNumMastersMap);
		for(Long partitionId : secondWaterFillingStrategy)
		{
			partitionIdToNumMastersMapCopy.put(partitionId, partitionIdToNumMastersMapCopy.get(partitionId) + 1);
		}

		for(Long partitionId : partitionIdToNumMastersMapCopy.keySet())
		{
			assertTrue(partitionIdToNumMastersMapCopy.get(partitionId).intValue() == 3 || partitionIdWithTwoMasters.equals(partitionId));
		}

		Map<Long, Long> preExistingStrategyMap = new HashMap<Long, Long>();
		for(Long partitionId : manager.getAllPartitionIds())
		{
			preExistingStrategyMap.put(partitionId * 2, partitionId);
		}

		List<Long> thirdWaterFillingStrategy = strategy.getWaterFillingStrategyOfPartitions(new HashSet<Long>(), preExistingStrategyMap, 7);
		partitionIdToNumMastersMapCopy = new HashMap<Long, Integer>(partitionIdToNumMastersMap);
		for(Long partitionId : thirdWaterFillingStrategy)
		{
			partitionIdToNumMastersMapCopy.put(partitionId, partitionIdToNumMastersMapCopy.get(partitionId) + 1);
		}
		for(Long partitionId : partitionIdToNumMastersMapCopy.keySet())
		{
			assertTrue(partitionIdToNumMastersMapCopy.get(partitionId).intValue() == 3);
		}

		preExistingStrategyMap.remove(partitionIdWithTwoMasters*2);
		List<Long> fourthWaterFillingStrategy = strategy.getWaterFillingStrategyOfPartitions(new HashSet<Long>(Arrays.asList(partitionIdWithTwoMasters)), preExistingStrategyMap, 6);
		partitionIdToNumMastersMapCopy = new HashMap<Long, Integer>(partitionIdToNumMastersMap);
		for(Long partitionId : fourthWaterFillingStrategy)
		{
			partitionIdToNumMastersMapCopy.put(partitionId, partitionIdToNumMastersMapCopy.get(partitionId) + 1);
		}
		for(Long partitionId : partitionIdToNumMastersMapCopy.keySet())
		{
			assertTrue(partitionIdToNumMastersMapCopy.get(partitionId).intValue() == 3 || partitionId.equals(partitionIdWithTwoMasters));
		}
	}

	@Test
	public void testGetUserMigrationStrategy()
	{
		SparManager manager = TestUtils.getStandardManager();
		SparMigrationStrategy strategy = new SparMigrationStrategy(manager);

		//TODO: do this
	}
}