package ch.zhaw.mapreduce;

import java.util.List;

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
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask der gestoppt werden soll und dessen zugehörige
	 *            Daten gelöscht werden sollen.
	 * @param workerTaskUUID
	 *            Die eindeutige ID des WorkerTasks der gestoppt und desssen zugehörige Daten
	 *            gelöscht werden sollen.
	 */
	void stopCurrentTask(String mapReduceUUID, String taskUUID);

	/**
	 * Gibt die derzeit auf dem Worker gespeicherten Strings zurueck
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden
	 *            sollen.
	 * @param workerTaskUUID
	 *            Die eindeutige ID des WorkerTasks desssen zugehörige Daten gelöscht werden sollen.
	 */
	List<String> getReduceResult(String mapReduceTaskUID, String workerTaskUUID);

	/**
	 * Gibt die derzeit auf dem Worker gespeicherten KeyValue Pairs zurück
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden
	 *            sollen.
	 * @param workerTaskUUID
	 *            Die eindeutige ID des WorkerTasks desssen zugehörige Daten gelöscht werden sollen.
	 */
	List<KeyValuePair> getMapResult(String mapReduceTaskUID, String workerTaskUUID);

	/**
	 * Räumt alle Resultate zu einem speziellen auf dem Worker ausgeführten Task auf
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten gelöscht werden
	 *            sollen.
	 * @param workerTaskUUID
	 *            Die eindeutige ID des WorkerTasks desssen zugehörige Daten gelöscht werden sollen.
	 */
	void cleanSpecificResult(String mapReduceTaskUID, String workerTaskUUID);

	/**
	 * Räumt in einem Worker alle Resultate auf (Speicher freigeben). Zu diesem Zeitpunkt ist die
	 * Map- und Reduce-Phase abgeschlossen.
	 * 
	 * @param mapReduceTaskUUID
	 *            diese Berechnung ist fertig
	 */
	void cleanAllResults(String mapReduceTaskUUID);

}
