package ch.zhaw.mapreduce.plugins.socket.impl;

import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;
import ch.zhaw.mapreduce.plugins.socket.SocketAgentResultFactory;
import ch.zhaw.mapreduce.plugins.socket.TaskResult;

public class SocketAgentResultFactoryImpl implements SocketAgentResultFactory {

	@Override
	public SocketAgentResult createFromTaskResult(String mapReduceTaskUuid, String taskUuid, TaskResult result) {
		if (result.wasSuccessful()) {
			return new SocketAgentResultImpl(mapReduceTaskUuid, taskUuid, result.getResult());
		} else {
			return new SocketAgentResultImpl(mapReduceTaskUuid, taskUuid, result.getException());
		}
	}

	@Override
	public SocketAgentResult createFromException(String mapReduceTaskUuid, String taskUuid, Exception e) {
		return new SocketAgentResultImpl(mapReduceTaskUuid, taskUuid, e);
	}

}
