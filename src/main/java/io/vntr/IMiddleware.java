package io.vntr;

import io.vntr.User;

public interface IMiddleware
{
	void addUser(User user);
	void removeUser(Long userId);

	void befriend(Long smallerUserId, Long largerUserId);
	void unfriend(Long smallerUserId, Long largerUserId);

	void addPartition();
	void removePartition(Long partitionId);
}