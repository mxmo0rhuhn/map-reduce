package ch.zhaw.mapreduce;

import java.util.List;

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
	 * @param mrUuid
	 *            die MapReduceID
	 * @param inputUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @param key
	 *            der Schlüssel für diese Berechnung
	 * @param value
	 *            der Wert dieser Berechnung
	 */
	void storeMap(String mrUuid, String inputUuid, String key, String value);

	/**
	 * Speichert ein Resultat von einer Berechnung.
	 * 
	 * @param mrUuid
	 *            die MapReduceID
	 * @param inputUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @param result
	 *            das Resultat dieser Berechnung
	 */
	void storeReduce(String mrUuid, String inputUuid, String result);

	/**
	 * Liefert das gespeicherte Resultat einer Reduce-Berechnung (Instruction) für diese MapReduceTaskID und Input-ID.
	 * 
	 * @param mrUuid
	 *            die MapReduceID
	 * @param inputUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @return das gespeicherte Resultat falls vorhanden, sonst null
	 */
	List<String> getReduce(String mrUuid, String inputUuid);

	/**
	 * Liefert die gespeicherten Resultate einer Map-Berechnung (Instruction) für diese MapReduceTaskID und Input-ID.
	 * 
	 * @param mrUuid
	 *            die MapReduceID
	 * @param inputUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @return die gespeicherten Resultate falls vorhanden, sonst null
	 */
	List<KeyValuePair<String, String>> getMap(String mrUuid, String inputUuid);

	/**
	 * Ersetzt das Map-Zswischen-Resultat falls vorhanden. Dies ist notwendig, wenn das Resultat einer Map-Berechnung
	 * durch die Combiner Instruction zusammengefasst wird.
	 * 
	 * @param mrUuid
	 *            die MapReduceID
	 * @param inputUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @param afterCombining
	 *            das kombiniert Resultat der Map-Berechnung.
	 * @throws IllegalArgumentException
	 *             falls kein Resultat für diese MapReduceID/inputUuid Kombination existiert
	 */
	void replaceMap(String mrUuid, String inputUuid, List<KeyValuePair<String, String>> afterCombining)
			throws IllegalArgumentException;

	/**
	 * Löscht das Resultat dieser MapReduce Berechnung für diese Input ID.
	 * 
	 * @param mrUuid
	 *            die MapReduceID
	 * @param inputUuid
	 *            die ID vom Input für die Berechnung, die zu diesem Resultat geführt hat
	 * @throws IllegalArgumentException
	 *             falls kein Resultat für diese MapReduceID/inputUuid Kombination existiert
	 */
	void destroy(String mrUuid, String inputUuid) throws IllegalArgumentException;

}
