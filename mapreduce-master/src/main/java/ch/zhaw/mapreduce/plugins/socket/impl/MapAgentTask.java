package ch.zhaw.mapreduce.plugins.socket.impl;

import java.io.ByteArrayOutputStream;

import javax.inject.Inject;

import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.plugins.socket.AgentTask;

import com.google.inject.assistedinject.Assisted;

public class MapAgentTask implements AgentTask {
	
	private final String mapReduceTaskUuid;
	
	private final String taskUuid;
	
	private final ByteArrayOutputStream bos;
	
	@Inject
	MapAgentTask(@Assisted MapWorkerTask workerTask) {
		this.mapReduceTaskUuid = workerTask.getMapReduceTaskUuid();
		this.taskUuid = workerTask.getTaskUuid();
		workerTask.getMapInstruction()
		
	}

}
