package ch.zhaw.mapreduce.plugins.socket.impl;

import javax.annotation.Nullable;

import ch.zhaw.mapreduce.plugins.socket.AgentTask;


public final class MapAgentTask implements AgentTask {

	private static final long serialVersionUID = -4656085734200620211L;

	private final String mapReduceTaskUuid;

	private final String taskUuid;

	private final String mapInstructionName;

	private final byte[] mapInstruction;

	private final String combinerInstructionName;

	private final byte[] combinerInstruction;

	private final String input;

	MapAgentTask(String mapReduceTaskUuid, String taskUuid, String mapInstructionName, byte[] mapInstruction,
			@Nullable String combinerInstructionName, @Nullable byte[] combinerInstruction, String input) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
		this.mapInstructionName = mapInstructionName;
		this.mapInstruction = mapInstruction;
		this.combinerInstructionName = combinerInstructionName;
		this.combinerInstruction = combinerInstruction;
		this.input = input;
	}

	@Override
	public String getMapReduceTaskUuid() {
		return mapReduceTaskUuid;
	}

	@Override
	public String getTaskUuid() {
		return taskUuid;
	}

	public String getMapInstructionName() {
		return mapInstructionName;
	}

	public byte[] getMapInstruction() {
		return mapInstruction;
	}

	public String getCombinerInstructionName() {
		return combinerInstructionName;
	}

	public byte[] getCombinerInstruction() {
		return combinerInstruction;
	}

	public String getInput() {
		return input;
	}
}
