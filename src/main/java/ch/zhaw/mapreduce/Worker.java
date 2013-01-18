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
	 * Lässt den Worker seine derzeitige Aufgabe bearbeiten. Nach dem Ausführen der Aufgabe muss sich der Worker bei
	 * seinem Pool melden.
	 * 
	 * @param task
	 *            den WorkerTask, der ausgefuert werden soll
	 */
	void execute(WorkerTask task);

	/**
	 * Bietet die Möglichkeit ein - wie auch immer geartetes Key Value Pair auf dem derzeitigen Worker zu speichern.
	 * Dieser muss sich um die Persistierung kümmern.
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden sollen.
	 * @param pair
	 *            Das Resultat
	 */
	void storeMapResult(String mapReduceTaskUID, KeyValuePair pair);

	/**
	 * Bietet die Möglichkeit ein - wie auch immer geartetes Key Value Pair auf dem derzeitigen Worker zu speichern.
	 * Dieser muss sich um die Persistierung kümmern.
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden sollen.
	 * @param pair
	 *            Das Resultat
	 */
	void storeReduceResult(String mapReduceTaskUID, KeyValuePair pair);

	/**
	 * Gibt die derzeit auf dem Worker gespeicherten KeyValue Pairs zurück
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden sollen.
	 */
	List<KeyValuePair> getReduceResults(String mapReduceTaskUID);

	/**
	 * Gibt die derzeit auf dem Worker gespeicherten KeyValue Pairs zurück
	 * 
	 * @param mapReduceTaskUID
	 *            Die eindeutige ID des MapReduceTask desssen zugehörige Daten zurückgegeben werden sollen.
	 */
	List<KeyValuePair> getMapResults(String mapReduceTaskUID);

	/**
	 * Räumt in einem Worker alle Resultate auf (Speicher freigeben). Zu diesem Zeitpunkt ist die Map- und Reduce-Phase
	 * abgeschlossen.
	 * 
	 * @param mapReduceTaskUUID
	 *            diese Berechnung ist fertig
	 */
	void cleanAllResults(String mapReduceTaskUUID);

	/**
	 * Ersetzt das Resultat einer Map-Berechnung mit dem neuen Resultat. Diese Methode wird typischerweise nach der
	 * Combine-Phase aufgerufen um das urspruengliche Resultat mit dem kombinierten auszuwechseln.
	 * 
	 * @param mapReduceTaskUID
	 *            zugehoerige globale ID
	 * @param newResult
	 *            neues Resultat
	 */
	void replaceMapResult(String mapReduceTaskUID, List<KeyValuePair> newResult);
}
