package ch.zhaw.mapreduce.plugins.socket.impl;

import javax.annotation.Nullable;

import ch.zhaw.mapreduce.plugins.socket.AgentTask;


/**
 * Task mit MapInstruction, optinaler CombinerInstruction und Input, der über das Netz zu einem Client/Worker gesandt wird.
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public final class MapAgentTask implements AgentTask {

	private static final long serialVersionUID = -4656085734200620211L;

	private final String taskUuid;

	private final String mapInstructionName;

	private final byte[] mapInstruction;

	/**
	 * Optional. Kann null sein
	 */
	private final String combinerInstructionName;

	/**
	 * Optional. Kann null sein
	 */
	private final byte[] combinerInstruction;

	private final String input;

	MapAgentTask(String taskUuid, String mapInstructionName, byte[] mapInstruction,
			@Nullable String combinerInstructionName, @Nullable byte[] combinerInstruction, String input) {
		this.taskUuid = taskUuid;
		this.mapInstructionName = mapInstructionName;
		this.mapInstruction = mapInstruction;
		this.combinerInstructionName = combinerInstructionName;
		this.combinerInstruction = combinerInstruction;
		this.input = input;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskUuid() {
		return taskUuid;
	}

	/**
	 * Name der Klasse, die die MapInstruction implementiert
	 */
	public String getMapInstructionName() {
		return mapInstructionName;
	}

	/**
	 * Byte Code der Klasse, die die MapInstruction implementiert
	 */
	public byte[] getMapInstruction() {
		return mapInstruction;
	}

	/**
	 * Name der Klasse, die die CombinerInstruction implementiert
	 */
	public String getCombinerInstructionName() {
		return combinerInstructionName;
	}

	/**
	 * Byte Code der Klasse, die die CombinerInstruction implementiert
	 */
	public byte[] getCombinerInstruction() {
		return combinerInstruction;
	}

	/**
	 * Input für die MapInstruction
	 */
	public String getInput() {
		return input;
	}
}
