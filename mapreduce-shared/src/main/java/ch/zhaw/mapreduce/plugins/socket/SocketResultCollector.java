package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.ConcurrentMap;

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
	 * Ein ResultCollectorObserver (typischerweise SocketWorker) registriert sich beim SocketResultCollector und wir
	 * benachrichtigt, sobald das Resultat für einen bestimmten Task angekommen ist.
	 * 
	 */
	Boolean registerObserver(String taskUuid, SocketResultObserver observer);

	/** 
	 * Liefert eine Referenz auf alle Resultat-Stati. Diese Methde ist für den Cleaner-Task gedacht.
	 */
	ConcurrentMap<String, ResultState> getResultStates();

}
