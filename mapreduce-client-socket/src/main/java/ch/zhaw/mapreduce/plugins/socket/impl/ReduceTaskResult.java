package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;

import ch.zhaw.mapreduce.plugins.socket.TaskResult;

import com.google.inject.assistedinject.Assisted;

public final class ReduceTaskResult implements TaskResult {
	
	private final String mapReduceTaskUuid;
	
	private final String taskUuid;

	private final List<String> result;

	private final Exception exception;
	
	ReduceTaskResult(@Assisted("mapReduceTaskUuid") String mapReduceTaskUuid,
			@Assisted("taskUuid") String taskUuid, @Assisted Exception e) {
		this(mapReduceTaskUuid, taskUuid, e, null);
	}

	ReduceTaskResult(@Assisted("mapReduceTaskUuid") String mapReduceTaskUuid,
			@Assisted("taskUuid") String taskUuid, @Assisted List<String> result) {
		this(mapReduceTaskUuid, taskUuid, null, result);
	}

	private ReduceTaskResult(String mapReduceTaskUuid, String taskUuid, Exception exception, List<String> result) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
		this.exception = exception;
		this.result = result;
	}

	@Override
	public boolean wasSuccessful() {
		return this.exception == null;
	}

	@Override
	public Exception getException() {
		return this.exception;
	}

	@Override
	public List<?> getResult() {
		return this.result;
	}

	@Override
	public String getMapReduceTaskUuid() {
		return this.mapReduceTaskUuid;
	}

	@Override
	public String getTaskUuid() {
		return this.taskUuid;
	}

}
