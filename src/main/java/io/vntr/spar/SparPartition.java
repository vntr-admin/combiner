package io.vntr.spar;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SparPartition
{
	private Map<Long, SparUser> idToMasterMap = new HashMap<Long, SparUser>();
	private Map<Long, SparUser> idToReplicaMap = new HashMap<Long, SparUser>();
	private Long id;

	public SparPartition(Long id) { this.id = id; }

	public void addMaster(SparUser user)
	{
		idToMasterMap.put(user.getId(), user);
	}

	public User removeMaster(Long id)
	{
		return idToMasterMap.remove(id);
	}

	public void addReplica(SparUser user)
	{
		idToReplicaMap.put(user.getId(), user);
	}

	public User removeReplica(Long id)
	{
		return idToReplicaMap.remove(id);
	}

	public SparUser getMasterById(Long userId)
	{
		return idToMasterMap.get(userId);
	}

	public SparUser getReplicaById(Long userId)
	{
		return idToReplicaMap.get(userId);
	}

	public int getNumMasters()
	{
		return idToMasterMap.size();
	}

	public int getNumReplicas()
	{
		return idToReplicaMap.size();
	}

	public Set<Long> getIdsOfMasters()
	{
		return idToMasterMap.keySet();
	}

	public Set<Long> getIdsOfReplicas()
	{
		return idToReplicaMap.keySet();
	}

	public Long getId() { return id; }
}