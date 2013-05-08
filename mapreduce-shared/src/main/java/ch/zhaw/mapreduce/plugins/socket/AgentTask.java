package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;

/**
 * Ein AgentTask ist die serialisierte Form eines Task. Auf dem Master heisst diese Struktur WorkerTask und sie wird in
 * einen AgentTask umgewandelt um über Netzt geschickt werden zu können. Deshalb muss diese Klasse, sowie alle
 * Attribute, serialisierbar sein.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface AgentTask extends Serializable {

	/**
	 * Globale Berechungs UUID
	 */
	public abstract String getMapReduceTaskUuid();

	/**
	 * UUID von diesem Task
	 */
	public abstract String getTaskUuid();

}
