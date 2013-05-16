package ch.zhaw.mapreduce;


/**
 * Stellt einen generischen Worker der eine Aufgabe annehmen kann dar.
 * 
 * @author Max
 * 
 */
public interface Worker {

	/**
	 * Lässt den Worker seine derzeitige Aufgabe bearbeiten. Nach dem Ausführen der Aufgabe muss
	 * sich der Worker bei seinem Pool melden.
	 * 
	 * @param task
	 *            den WorkerTask, der ausgefuert werden soll
	 */
	void executeTask(WorkerTask task);

	/**
	 * Hält die derzeitige Berechnung an, falls die Berechnung noch den im Input mitgegebenen
	 * Parametern entspricht. Z.B. wenn die Resultate der Berechnung nicht weiterhin von Relevanz
	 * sind.
	 * 
	 * @param workerTaskUUID
	 *            Die eindeutige ID des WorkerTasks der gestoppt und desssen zugehörige Daten
	 *            gelöscht werden sollen.
	 */
	void stopCurrentTask(String taskUUID);
}
