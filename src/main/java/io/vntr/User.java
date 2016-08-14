package io.vntr;

import java.util.HashSet;
import java.util.Set;

public class User
{
	private String name;
	private Long id;
	private Set<Long> friendIDs;

	public User(String name, Long id)
	{
		this.name = name;
		this.id = id;
		this.friendIDs = new HashSet<Long>();
	}

	public String getName()          { return name; }
	public void setName(String name) { this.name = name; }

	public Long getId()              { return id; }
	public void setId(Long id)       { this.id = id; }

	public void befriend(Long friendId) { friendIDs.add(friendId); }
	public void unfriend(Long friendId) { friendIDs.remove(friendId); }

	public Set<Long> getFriendIDs() { return friendIDs; }

	@Override
	public String toString()
	{
		return id + "|" + name + "|friends:" + friendIDs.toString(); 
	}
}