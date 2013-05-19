package ch.zhaw.mapreduce;

import java.util.List;
import java.util.Map;

/**
 * 
 * Eine Persistence kann in einem {@link Context} verwendet werden, wenn der Kontext nicht alle (Zwischen-) Resultate
 * In-Memory halten kann. Dann kann eine bestimmte Persistence die Werte auf das Dateisystem, eine Datenbank oder
 * ähnlich schreiben.
 * 
 * Eine Persistence ist global verwendbar, da sie jedes gespeicherte Resultat mit einer Berechnungs (MapReduce) ID
 * versieht.
 * 
 * @author Max Schrimpf (schrimax)
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface Persistence {

	/**
	 * Speichert ein Resultat von einer Berechnung.
	 * 
	 * @param taskUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @param key
	 *            der Schlüssel für diese Berechnung
	 * @param value
	 *            der Wert dieser Berechnung
	 * @return true, wenn das speichern funktioniert hat, sonst false
	 */
	boolean storeMapResults(String taskUuid, List<KeyValuePair> mapResults);

	/**
	 * Speichert ein Resultat von einer Berechnung.
	 * 
	 * @param taskUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @param result
	 *            das Resultat dieser Berechnung
	 * @return true, wenn das speichern funktioniert hat, sonst false
	 */
	boolean storeReduceResults(String taskUuid, String key, List<String> results);

	/**
	 * Liefert das gespeicherte Resultat einer Reduce-Berechnung (Instruction) für diese MapReduceTaskID und Input-ID.
	 * 
	 * @return das gespeicherte Resultat falls vorhanden, sonst null
	 */
	Map<String, List<String>> getReduceResults();

	/**
	 * Liefert die gespeicherten Resultate einer Map-Berechnung (Instruction) für diese MapReduceTaskID und Input-ID.
	 * 
	 * @return die gespeicherten Resultate falls vorhanden, sonst null
	 */
	List<KeyValuePair> getMapResults();

	/**
	 * Löscht das Resultat dieser MapReduce Berechnung für diese Input ID.
	 * 
	 * @param taskUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @throws IllegalArgumentException
	 *             falls kein Resultat für diese MapReduceID/inputUuid Kombination existiert
	 */
	boolean destroyMap(String taskUuid);

	/**
	 * Löscht das Resultat dieser MapReduce Berechnung für diese Input ID.
	 * 
	 * @param taskUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @throws IllegalArgumentException
	 *             falls kein Resultat für diese MapReduceID/inputUuid Kombination existiert
	 */
	boolean destroyReduce(String taskUuid);

	/**
	 * Löscht alle gespeicherten Resultate.
	 */
	boolean suicide();

}
