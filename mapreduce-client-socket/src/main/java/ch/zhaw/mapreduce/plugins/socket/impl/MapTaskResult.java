package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.plugins.socket.TaskResult;

import com.google.inject.assistedinject.Assisted;

public class MapTaskResult implements TaskResult {

	private final String taskUuid;

	private final List<KeyValuePair> result;

	private final Exception exception;

	MapTaskResult(@Assisted("taskUuid") String taskUuid, @Assisted Exception e) {
		this(taskUuid, e, null);
	}

	MapTaskResult(@Assisted("taskUuid") String taskUuid, @Assisted List<KeyValuePair> result) {
		this(taskUuid, null, result);
	}

	private MapTaskResult(String taskUuid, Exception exception, List<KeyValuePair> result) {
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
	public List<KeyValuePair> getResult() {
		return this.result;
	}

	@Override
	public String getTaskUuid() {
		return this.taskUuid;
	}

}
