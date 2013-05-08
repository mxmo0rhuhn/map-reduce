package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.WorkerTask;

/**
 * Diese Factory wird mit Guice/AssistedInject verwaltet.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface AgentTaskFactory {

	/**
	 * Erstellt einen neuen AgentTask, welcher auf dem Client ausführt werden kann, basierend auf dem serverseitigen
	 * WorkerTask.
	 * 
	 * @param workerTask
	 *            Task auf dem Server
	 * @return task für den client
	 */
	AgentTask createAgentTask(WorkerTask workerTask);

}
