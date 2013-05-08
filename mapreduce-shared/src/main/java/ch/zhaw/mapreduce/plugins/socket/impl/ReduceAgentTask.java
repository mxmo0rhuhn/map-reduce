package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;

import javax.inject.Inject;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.plugins.socket.AgentTask;

/**
 * Task mit ReduceInstruction und Inputs, der über das Netz zu einem Client/Worker gesandt wird.
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public final class ReduceAgentTask implements AgentTask {

	private static final long serialVersionUID = -1795450051047132175L;
	
	private final String mapReduceTaskUuid;
	
	private final String taskUuid;
	
	private final String riName;
	
	private final byte[] ri;
	
	private final String key;
	
	private final List<KeyValuePair> values;

	@Inject
	ReduceAgentTask(String mapReduceTaskUuid, String taskUuid, String riName, byte[] ri, String key,
			List<KeyValuePair> values) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
		this.riName = riName;
		this.ri = ri;
		this.key = key;
		this.values = values;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getMapReduceTaskUuid() {
		return this.mapReduceTaskUuid;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskUuid() {
		return this.taskUuid;
	}

	/**
	 * Name der Klasse, die die ReduceInstruction implementiert
	 */
	public String getReduceInstructionName() {
		return this.riName;
	}

	/**
	 * Byte Code der ReduceInstruction Implementation
	 */
	public byte[] getReduceInstruction() {
		return this.ri;
	}

	/**
	 * Schlüssel zum Reduzieren
	 */
	public String getKey() {
		return this.key;
	}

	/**
	 * Werte zum Reduzieren
	 */
	public List<KeyValuePair> getValues() {
		return this.values;
	}

}
