package ch.zhaw.mapreduce.workers;

import java.util.List;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.WorkerTask;

/**
 * Stellt einen generischen Worker der eine Aufgabe annehmen kann dar.
 * 
 * @author Max
 * 
 */
public interface Worker {

	/**
	 * Lässt den Worker seine derzeitige Aufgabe bearbeiten. Nach dem Ausführen der Aufgabe muss sich der Worker bei
	 * seinem Pool melden.
	 * 
	 * @param task
	 *            den WorkerTask, der ausgefuert werden soll
	 */
	void executeTask(WorkerTask task);

	/**
	 * Gibt die derzeit auf dem Worker gespeicherten KeyValue Pairs zurück
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden sollen.
	 */
	List<KeyValuePair> getReduceResult(String mapReduceTaskUID, String inputUID);

	/**
	 * Gibt die derzeit auf dem Worker gespeicherten KeyValue Pairs zurück
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden sollen.
	 */
	List<KeyValuePair> getMapResult(String mapReduceTaskUID, String inputUID);

	/**
	 * Räumt in einem Worker alle Resultate auf (Speicher freigeben). Zu diesem Zeitpunkt ist die Map- und Reduce-Phase
	 * abgeschlossen.
	 * 
	 * @param mapReduceTaskUUID
	 *            diese Berechnung ist fertig
	 */
	void cleanAllResults(String mapReduceTaskUUID);

}
