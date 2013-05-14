package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import ch.zhaw.mapreduce.KeyValuePair;

/**
 * Der SocketResultCollector lebt auf dem Master und wartet auf Worker die ihm Resultate geben. Hier werden alle
 * Resultate gesammelt.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface SocketResultCollector {

	/**
	 * Speichert das Resultat von einem Agent
	 * 
	 * @param res
	 *            Resultat vom Agent
	 */
	void pushResult(SocketAgentResult res);

	/**
	 * Liefert Resultate der Reduce Phase, in der Annahme, dass diese bereits von einem Agent geliefert wurden.
	 */
	List<String> getReduceResult(String mapReduceTaskUuid, String taskUuid);

	/**
	 * Liefert Resultate der Map Phase, in der Annahme, dass diese bereits von einem Agent geliefert wurden.
	 */
	List<KeyValuePair> getMapResult(String mapReduceTaskUuid, String taskUuid);

	/**
	 * Löscht Resultate dieser ID
	 */
	void cleanAllResults(String mapReduceTaskUuid);

	/**
	 * Löscht die Resultate dieser IDs
	 */
	void cleanResult(String mapReduceTaskUuid, String taskUuid);

	/**
	 * Ein ResultCollectorObserver (typischerweise SocketWorker) registriert sich beim SocketResultCollector und wir
	 * benachrichtigt, sobald das Resultat für einen bestimmten Task angekommen ist.
	 * 
	 */
	Boolean registerObserver(String mapReduceTaskUuid, String taskUuid, SocketResultObserver observer);

	/** 
	 * Liefert eine Referenz auf alle Resultat-Stati. Diese Methde ist für den Cleaner-Task gedacht.
	 */
	ConcurrentMap<String, ResultState> getResultStates();

}
