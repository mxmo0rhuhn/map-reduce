package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.WorkerTask;

/**
 * Diese Factory zum erstellen voon AgentTasks
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface AgentTaskFactory {

	/**
	 * Erstellt basierend auf einem Serverseitigen WorkerTask einen AgentTask, welcher auf einem SocketAgent ausgeführt
	 * werden kann.
	 */
	AgentTask createAgentTask(WorkerTask workerTask);

}
