package ch.zhaw.mapreduce;

import java.util.List;

import ch.zhaw.mapreduce.workers.Worker;


/***
 * Eine WorkerTask ist eine Aufgabe die von einem Worker ausgeführt werden kann.
 * 
 * @author Max
 * 
 */
public interface WorkerTask {

	// Alle möglichen Zustände in denen sich Worker befinden kann
	public enum State {
		INITIATED, // erstellt
		ENQUEUED, // dem pool zur ausfuehrung ueberreicht
		INPROGRESS, // pool hat task akzeptiert
		COMPLETED, // completed
		FAILED , // failed
		ABORTED // computation stopped from outside
	}

	/**
	 * Führt die Aufgabe, die der Worker erfüllen soll aus.
	 * 
	 * @param processingWorker
	 *            der Worker auf dem die Aufgabe ausgeführt wird.
	 */
	void runTask(Context ctx);

	/***
	 * Gibt den Zustand der Aufgabe die erfüllt werden soll zurück.
	 * 
	 * @return der Zustand
	 */
	State getCurrentState();

	/**
	 * Die ID dieses Worker Tasks.
	 * @return
	 */
	String getUUID();

	/**
	 * Liefert die MapReduceTask ID zu der dieser Task gehoert
	 * 
	 * @return die MapReduceTask ID zu der dieser Task gehoert
	 */
	String getMapReduceTaskUUID();

	void setWorker(Worker worker);
	
	Worker getWorker();

	List<KeyValuePair> getResults(String mapReduceTaskUUID);
	
	String getInput();
}
