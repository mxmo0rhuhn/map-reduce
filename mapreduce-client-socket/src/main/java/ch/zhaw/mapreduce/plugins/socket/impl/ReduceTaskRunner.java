package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;

import javax.inject.Inject;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.plugins.socket.SocketTaskResult;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;

import com.google.inject.assistedinject.Assisted;

public final class ReduceTaskRunner implements TaskRunner {

	private final String mapReduceTaskUuid;

	private final String taskUuid;

	private final ReduceInstruction redInstr;

	private final String key;

	private final List<KeyValuePair> values;

	@Inject
	ReduceTaskRunner(@Assisted("mapReduceTaskUuid") String mapReduceTaskUuid, @Assisted("taskUuid") String taskUuid,
			@Assisted ReduceInstruction redInstr, @Assisted("key") String key, @Assisted List<KeyValuePair> values) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
		this.redInstr = redInstr;
		this.key = key;
		this.values = values;
	}

	@Override
	public SocketTaskResult runTask() {
		return null;
	}

}
