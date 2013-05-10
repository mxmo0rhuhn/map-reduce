package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;

import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;

public final class SocketAgentResultImpl implements SocketAgentResult {

	private static final long serialVersionUID = -5674131321896450221L;

	private final String mapReduceTaskUuid;

	private final String taskUuid;

	private final List<?> result;

	private final Exception exception;

	SocketAgentResultImpl(String mapReduceTaskUuid, String taskUuid, Exception exception) {
		this(mapReduceTaskUuid, taskUuid, null, exception);
	}

	SocketAgentResultImpl(String mapReduceTaskUuid, String taskUuid, List<?> result) {
		this(mapReduceTaskUuid, taskUuid, result, null);
	}

	private SocketAgentResultImpl(String mapReduceTaskUuid, String taskUuid, List<?> result, Exception exception) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
		this.result = result;
		this.exception = exception;
	}

	@Override
	public String getMapReduceTaskUuid() {
		return this.mapReduceTaskUuid;
	}

	@Override
	public String getTaskUuid() {
		return this.taskUuid;
	}

	@Override
	public boolean wasSuccessful() {
		return this.result != null;
	}

	@Override
	public Exception getException() {
		return this.exception;
	}

	@Override
	public List<?> getResult() {
		return this.result;
	}

}
