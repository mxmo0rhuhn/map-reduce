package ch.zhaw.mapreduce;



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

	/**
	 * Weist der Aufgabe einen verarbeitenden Worker zu
	 * @param worker
	 */
	void setWorker(Worker worker);
	
	/** 
	 * Gibt den Worker der diese Aufgabe bearbeitet zurück
	 * @return der Worker
	 */
	Worker getWorker();

	/**
	 * Gibt den von diesem Worker bearbeiteten Input zurück
	 * @return
	 */
	String getInput();
	
	/**
	 * Bricht die Aufgabe ab, weil die Berechnung fertig ist (z.B. ein anderer Worker war schneller).
	 */
	void abort();
	
	/**
	 * Task wurde dem Pool uebergeben
	 */
	void enqueued();

	void failed();

	void started();

	void completed();

	void aborted();
}
