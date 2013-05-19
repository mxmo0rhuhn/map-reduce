package ch.zhaw.mapreduce;

import java.util.List;




/***
 * Eine WorkerTask ist eine Aufgabe die von einem Worker ausgeführt werden kann.
 * 
 * @author Max
 * 
 */
public interface WorkerTask {

	// Alle möglichen Zustände in denen sich Worker befinden kann
	public static enum State {
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
	String getTaskUuid();

	/**
	 * Gibt den von diesem Worker bearbeiteten Input zurück
	 * @return
	 */
	String getInput();
	
	/**
	 * Bricht die Aufgabe ab, weil die Berechnung fertig ist (z.B. ein anderer Worker war schneller).
	 */
	void abort();
	
	Persistence getPersistence();
	
	/**
	 * Task wurde dem Pool uebergeben
	 */
	void enqueued();

	void fail();

	void started();

	void successful(List<?> result);

}
