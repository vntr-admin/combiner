package io.vntr.spar;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class SparManager
{
	private int minNumReplicas;

	private static final Long defaultStartingId = 1L;

	private SortedMap<Long, SparPartition> partitionIdToPartitionMap;

	private Map<Long, Long> userIdToMasterPartitionIdMap = new HashMap<Long, Long>();

	public SparManager(int minNumReplicas)
	{
		this.minNumReplicas = minNumReplicas;
		this.partitionIdToPartitionMap = new TreeMap<Long, SparPartition>();
	}

	public int getMinNumReplicas()
	{
		return minNumReplicas;
	}

	public SparPartition getPartitionById(Long id)
	{
		return partitionIdToPartitionMap.get(id);
	}

	public SparUser getUserMasterById(Long id)
	{
		Long partitionId = userIdToMasterPartitionIdMap.get(id);
		if(partitionId != null)
		{
			SparPartition partition = getPartitionById(partitionId);
			if(partition != null)
			{
				return partition.getMasterById(id);
			}
		}
		return null;
	}

	public int getNumUsers()
	{
		return userIdToMasterPartitionIdMap.size();
	}

	public Set<Long> getAllPartitionIds()
	{
		return partitionIdToPartitionMap.keySet();
	}

	public Set<Long> getAllUserIds()
	{
		return userIdToMasterPartitionIdMap.keySet();
	}

	public void addUser(User user)
	{
		Long masterPartitionId = getPartitionIdWithFewestMasters();

		SparUser sparUser = new SparUser(user.getName(), user.getId());
		sparUser.setMasterPartitionId(masterPartitionId);
		sparUser.setPartitionId(masterPartitionId);

		addUser(sparUser, masterPartitionId);

		for(Long id : getPartitionsToAddInitialReplicas(masterPartitionId))
		{
			addReplica(sparUser, id);
		}
	}

	void addUser(SparUser user, Long masterPartitionId)
	{
		getPartitionById(masterPartitionId).addMaster(user);
		userIdToMasterPartitionIdMap.put(user.getId(), masterPartitionId);
	}

	public void removeUser(Long userId)
	{
		SparUser user = getUserMasterById(userId);

		//Remove user from relevant partitions
		getPartitionById(user.getMasterPartitionId()).removeMaster(userId);
		for(Long replicaPartitionId : user.getReplicaPartitionIds())
		{
			getPartitionById(replicaPartitionId).removeReplica(userId);
		}

		//Remove user from userIdToMasterPartitionIdMap
		userIdToMasterPartitionIdMap.remove(userId);

		//Remove friendships
		for(Long friendId : user.getFriendIDs())
		{
			SparUser friendMaster = getUserMasterById(friendId);
			friendMaster.unfriend(userId);

			for(Long friendReplicaPartitionId : friendMaster.getReplicaPartitionIds())
			{
				SparPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
				friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
			}
		}
	}

	public Long addPartition()
	{
		Long newId = partitionIdToPartitionMap.isEmpty() ? defaultStartingId : partitionIdToPartitionMap.lastKey() + 1;
		addPartition(newId);
		return newId;
	}

	void addPartition(Long partitionId)
	{
		partitionIdToPartitionMap.put(partitionId, new SparPartition(partitionId));
	}

	public void removePartition(Long id)
	{
		partitionIdToPartitionMap.remove(id);
	}

	public void addReplica(SparUser user, Long destinationPartitionId)
	{
		SparUser replicaOfUser = addReplicaNoUpdates(user, destinationPartitionId);

		//Update the replicaPartitionIds to reflect this addition
		replicaOfUser.addReplicaPartitionId(destinationPartitionId);
		for(Long partitionId : user.getReplicaPartitionIds())
		{
			partitionIdToPartitionMap.get(partitionId).getReplicaById(user.getId()).addReplicaPartitionId(destinationPartitionId);
		}
		user.addReplicaPartitionId(destinationPartitionId);
	}

	SparUser addReplicaNoUpdates(SparUser user, Long destinationPartitionId)
	{
		SparUser replicaOfUser = user.clone();
		replicaOfUser.setPartitionId(destinationPartitionId);
		partitionIdToPartitionMap.get(destinationPartitionId).addReplica(replicaOfUser);
		return replicaOfUser;
	}

	public void removeReplica(SparUser user, Long removalPartitionId)
	{
		//Delete it from each replica's replicaPartitionIds
		for(Long currentReplicaPartitionId : user.getReplicaPartitionIds())
		{
			partitionIdToPartitionMap.get(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(removalPartitionId);
		}

		//Delete it from the master's replicaPartitionIds
		user.removeReplicaPartitionId(removalPartitionId);

		//Actually remove the replica from the partition itself
		partitionIdToPartitionMap.get(removalPartitionId).removeReplica(user.getId());
	}

	public void moveUser(SparUser user, Long destinationPartitionId, Set<Long> replicasToAddInDestinationPartition, Set<Long> replicasToDeleteInSourcePartition)
	{
		//Step 1: move the actual user
		Long userId = user.getId();
		Long oldPartitionId = user.getMasterPartitionId();
		SparPartition oldPartition = partitionIdToPartitionMap.get(user.getMasterPartitionId());
		SparPartition newPartition = partitionIdToPartitionMap.get(destinationPartitionId);
		oldPartition.removeMaster(userId);
		newPartition.addMaster(user);
		userIdToMasterPartitionIdMap.put(userId, destinationPartitionId);

		user.setMasterPartitionId(destinationPartitionId);
		user.setPartitionId(destinationPartitionId);

		for(Long replicaPartitionId : user.getReplicaPartitionIds())
		{
			SparUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
			replica.setMasterPartitionId(destinationPartitionId);
		}

		//Step 2: add the necessary replicas
		for(Long friendId : user.getFriendIDs())
		{
			if(userIdToMasterPartitionIdMap.get(friendId).equals(oldPartitionId))
			{
				addReplica(user, oldPartitionId);
				break;
			}
		}

		for(Long friendToReplicateId : replicasToAddInDestinationPartition)
		{
			addReplica(getUserMasterById(friendToReplicateId), destinationPartitionId);
		}

		//Step 3: remove unnecessary replicas
		//Possibilities:
		// (1) replica of user in destinationPartition
		// (2) replicas of user's friends in oldPartition with no other purpose
		// (3) [the replica of the new friend that prompted this move should already be accounted for in (2)]

		if(user.getReplicaPartitionIds().contains(destinationPartitionId))
		{
			if(user.getReplicaPartitionIds().size() > minNumReplicas)
			{
				removeReplica(user, destinationPartitionId);
			}
			else
			{
				//delete the replica in destinationPartition,  but add one in another partition that doesn't yet have one of this user
				addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
				removeReplica(user, destinationPartitionId);
			}
		}

		//delete the replica of the appropriate friends in oldPartition
		for(Long replicaIdToDelete : replicasToDeleteInSourcePartition)
		{
			removeReplica(getUserMasterById(replicaIdToDelete), oldPartitionId);
		}
	}

	public void befriend(SparUser smallerUser, SparUser largerUser)
	{
		smallerUser.befriend(largerUser.getId());
		largerUser.befriend(smallerUser.getId());

		for(Long replicaPartitionId : smallerUser.getReplicaPartitionIds())
		{
			partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
		}

		for(Long replicaPartitionId : largerUser.getReplicaPartitionIds())
		{
			partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
		}
	}

	public void unfriend(SparUser smallerUser, SparUser largerUser)
	{
		smallerUser.unfriend(largerUser.getId());
		largerUser.unfriend(smallerUser.getId());

		for(Long partitionId : smallerUser.getReplicaPartitionIds())
		{
			partitionIdToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
		}

		for(Long partitionId : largerUser.getReplicaPartitionIds())
		{
			partitionIdToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
		}
	}

	public void promoteReplicaToMaster(Long userId, Long partitionId)
	{
		SparPartition partition = partitionIdToPartitionMap.get(partitionId);
		SparUser user = partition.getReplicaById(userId);
		user.setMasterPartitionId(partitionId);
		user.removeReplicaPartitionId(partitionId);
		partition.addMaster(user);
		partition.removeReplica(userId);

		userIdToMasterPartitionIdMap.put(userId, partitionId);

		for(Long replicaPartitionId : user.getReplicaPartitionIds())
		{
			SparUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
			replica.setMasterPartitionId(partitionId);
			replica.removeReplicaPartitionId(partitionId);
		}
	}

	Long getPartitionIdWithFewestMasters()
	{
		int minMasters = Integer.MAX_VALUE;
		Long minId = -1L;

		for(Long id: partitionIdToPartitionMap.keySet())
		{
			int numMasters = getPartitionById(id).getNumMasters();
			if(numMasters < minMasters)
			{
				minMasters = numMasters;
				minId = id;
			}
		}

		return minId;
	}

	Long getRandomPartitionIdWhereThisUserIsNotPresent(SparUser user)
	{
		Set<Long> potentialReplicaLocations = new HashSet<Long>(partitionIdToPartitionMap.keySet());
		potentialReplicaLocations.remove(user.getMasterPartitionId());
		potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
		List<Long> list = new LinkedList<Long>(potentialReplicaLocations);
		return list.get((int) (list.size() * Math.random()));
	}

	Set<Long> getPartitionsToAddInitialReplicas(Long masterPartitionId)
	{
		List<Long> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<Long>(partitionIdToPartitionMap.keySet());
		partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
		return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
	}

	public static final String MASTER_SECTION_HEADER = "#MASTERS";
	public static final String REPLICAS_SECTION_HEADER = "#REPLICAS";
	public static final String FRIENSHIPS_SECTION_HEADER = "#FRIENSHIPS";
	public static final String NEWLINE = System.getProperty("line.separator");
	private static final byte[] NEWLINE_BYTES = NEWLINE.getBytes();
	private static final int friendshipsPerLine = 1000;

	public void export(OutputStream out) throws IOException
	{
		println(out, MASTER_SECTION_HEADER);
		for(Long partitionId : partitionIdToPartitionMap.keySet())
		{
			Set<Long> masters = getPartitionById(partitionId).getIdsOfMasters();
			println(out, partitionId + ": " + longSetToCSVString(masters));
		}

		println(out, "");
		println(out, REPLICAS_SECTION_HEADER);
		for(Long partitionId : partitionIdToPartitionMap.keySet())
		{
			Set<Long> replicas = getPartitionById(partitionId).getIdsOfReplicas();
			println(out, partitionId + ": " + longSetToCSVString(replicas));
		}

		println(out, "");
		println(out, FRIENSHIPS_SECTION_HEADER);
		int count = 0;
		for(Long userId : userIdToMasterPartitionIdMap.keySet())
		{
			Set<Long> friendIds = getUserMasterById(userId).getFriendIDs();
			for(Long friendId : friendIds)
			{
				if(userId.compareTo(friendId) < 0)
				{
					print(out, userId + "_" + friendId + ",");
					if(++count % friendshipsPerLine == 0)
					{
						println(out, "");
					}
				}
			}
		}

		out.close();
	}

	private static String longSetToCSVString(Set<Long> set)
	{
		StringBuilder builder = new StringBuilder();
		for(Iterator<Long> iter = set.iterator(); iter.hasNext(); )
		{
			builder.append(iter.next());
			if(iter.hasNext())
			{
				builder.append(",");
			}
		}
		return builder.toString();
	}

	private static final void println(OutputStream out, String str) throws IOException
	{
		out.write(str.getBytes());
		out.write(NEWLINE_BYTES);
	}

	private static final void print(OutputStream out, String str) throws IOException
	{
		out.write(str.getBytes());
	}

	public Map<Long, Set<Long>> getPartitionToUserMap() {
		Map<Long, Set<Long>> map = new HashMap<Long, Set<Long>>();
		for(Long pid : partitionIdToPartitionMap.keySet()) {
			map.put(pid, getPartitionById(pid).getIdsOfMasters());
		}
		return map;
	}

	public Long getEdgeCut() {
		long count = 0L;
		for(Long uid : userIdToMasterPartitionIdMap.keySet()) {
			SparUser user = getUserMasterById(uid);
			Long pid = user.getMasterPartitionId();
			for(Long friendId : user.getFriendIDs()) {
				if(!pid.equals(userIdToMasterPartitionIdMap.get(friendId))) {
					count++;
				}
			}
		}
		return count/2;
	}
}