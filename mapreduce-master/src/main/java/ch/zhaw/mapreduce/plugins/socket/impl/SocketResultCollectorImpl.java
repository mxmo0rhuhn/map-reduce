package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.plugins.socket.ResultState;
import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;
import ch.zhaw.mapreduce.plugins.socket.SocketResultObserver;
import de.root1.simon.annotation.SimonRemote;

/**
 * Der SocketResultCollector wird vom SocketAgent angesprochen, sobald dieser die Berechnung abgeschlossen hat. Dabei
 * wird dem SocketResultCollector vom SocketAgent das Resultat der Berechnung mitgeteilt, welches auf dem Server
 * gespeichert wird. Aussedem merkt sich der SocketResultCollector, welche Resultate angekommen sind.
 * 
 * Andererseits kann sich ein SocketWorker beim SocketResultCollector registrieren, sodass er notifiziert wird, sobald
 * das Resultat seiner Berechnung verfügbar ist. Für diesen Mechanismus wird die ResultState Klasse verwendet, welche
 * die Idee dahinter noch genauer erklärt.
 * 
 * Die Liste mit den verfügbaren Resultaten wird periodisch vom ResultCleanerTask aufgeräumt, weil z.B. ein SocketWorker
 * sterben könnte und dann das Resultat nie akzeptieren würde.
 * 
 * @see ResultState
 * 
 * @see ResultCleanerTaskTest
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
@SimonRemote(SocketResultCollector.class)
public final class SocketResultCollectorImpl implements SocketResultCollector {

	private static final Logger LOG = Logger.getLogger(SocketResultCollectorImpl.class.getName());

	private final ConcurrentMap<String, ResultState> results = new ConcurrentHashMap<String, ResultState>();

	/**
	 * Die Resultate vom SocketAgent werden mit der Persistence gespeichert, um später vom Master abgeholt zu werden.
	 */
	private final Persistence pers;

	@Inject
	SocketResultCollectorImpl(Persistence pers) {
		this.pers = pers;
	}

	@Override
	public void pushResult(SocketAgentResult res) {
		String taskUuid = res.getTaskUuid();
		LOG.entering(getClass().getName(), "pushResult", new Object[] { taskUuid });

		if (res.wasSuccessful()) {
			storeResult(res);
		} else {
			LOG.log(Level.WARNING, "Failed to run Task on Agent", res.getException());
		}
		notifySocketWorker(taskUuid, res.wasSuccessful());

		LOG.exiting(getClass().getName(), "pushResult");
	}

	/**
	 * Registriert einen Observer für das Resultat einer Berechnung. Wenn die Berechnung bereits verfügbar ist, wird der
	 * Observer sofort benachrichtig. Wenn das Resultat schon einmal Requested wurde, wird der vorherige Observer
	 * überschrieben.
	 * 
	 * @return null, wenn das resultat noch nicht verfügbar ist, sonst boolean mit true, wenn es erfolgreich war, sonst
	 *         false.
	 */
	@Override
	public Boolean registerObserver(String taskUuid, SocketResultObserver observer) {
		LOG.entering(getClass().getName(), "registerObserver", new Object[] { taskUuid, observer });
		ResultState newResultState = ResultState.requestedBy(observer);
		ResultState resultState = this.results.putIfAbsent(taskUuid, newResultState);
		Boolean result = null;
		if (resultState != null) {
			if (resultState.available()) {
				LOG.log(Level.FINE, "Result is already available for TaskUuid={0}, Success={1}",
						new Object[] { taskUuid, resultState.successful() });
				this.results.remove(taskUuid);
				result = Boolean.valueOf(resultState.successful());
				LOG.log(Level.FINE,
						"Notified Observer and Removed Result from List for TaskUuid={0}, Observer={1}",
						new Object[] { taskUuid, observer });
			} else if (resultState.requested()) {
				LOG.log(Level.WARNING, "Second Observer for same Result, TaskUuid={0}, Replacing Observer={1} with Observer={2}", new Object[] { taskUuid, resultState.requestedBy(), newResultState.requestedBy() });
				this.results.put(taskUuid, newResultState);
			} else {
				throw new IllegalStateException("ResultState not handled: " + resultState);
			}
		} else {
			LOG.log(Level.FINE, "Added Observer for TaskUuid={0}, Observer={1}", new Object[] { taskUuid, observer });
		}
		LOG.exiting(getClass().getName(), "registerObserver", result);
		return result;
	}

	/**
	 * Notifiziert den Observer, dass ein Resultat verfügbar ist, wenn ein Observer für diese Berechnung eingetragen
	 * ist. Wenn kein Observer eingetragen ist, d.h. das Resultat wurde noch nicht Requested, wird dies so in der Liste
	 * eingetragen. Wenn das Resultat schonmal als Available eingetragen wurde, wird der vorherige Zustand
	 * überschrieben.
	 * 
	 */
	private void notifySocketWorker(String taskUuid, boolean wasSuccessful) {
		LOG.entering(getClass().getName(), "notifySocketWorker", new Object[] { taskUuid, wasSuccessful });
		String key = taskUuid;
		ResultState newState = ResultState.resultAvailable(wasSuccessful);
		ResultState resultState = this.results.putIfAbsent(key, newState);
		if (null != resultState) {
			if (resultState.requested()) {
				LOG.log(Level.FINE, "Result was Requested TaskUuid={0}", new Object[] { taskUuid });
				SocketResultObserver observer = resultState.requestedBy();
				observer.resultAvailable(taskUuid, wasSuccessful);
			} else {
				this.results.put(key, newState);
				LOG.log(Level.WARNING, "Result was already Available. Replacing old State with new one for TaskUuid={0}", new Object[] { taskUuid });
			}
			LOG.log(Level.FINE, "Removing Result from List for TaskUuid={0}", new Object[] { taskUuid });
			this.results.remove(key);
		} else {
			LOG.log(Level.FINE, "Result for TaskUuid={0} now Available", new Object[] { taskUuid });
		}
		LOG.exiting(getClass().getName(), "notifySocketWorker");
	}

	private void storeResult(SocketAgentResult saRes) {
		List<?> res = saRes.getResult();
		if (res.isEmpty()) {
			LOG.info("Empy Result from SocketAgent");
		} else if (hasType(res, KeyValuePair.class)) {
			storeMap(saRes.getTaskUuid(), (List<KeyValuePair>) res);
		} else if (hasType(res, String.class)) {
			storeReduce(saRes.getTaskUuid(), (List<String>) res);
		} else {
			LOG.severe("Null Result from SocketAgent");
		}
	}

	/**
	 * Prüft, ob der Inhalt der Liste diesen Typ hat. Somit kann erkannt werden, ob es sich um ein Map- oder
	 * Reduce-Resultat handelt.
	 */
	boolean hasType(List<?> res, Class<?> klass) {
		return res != null && res.get(0).getClass().equals(klass);
	}

	void storeMap(String taskUuid, List<KeyValuePair> res) {
		LOG.entering(getClass().getName(), "storeMap", new Object[] { taskUuid });
		this.pers.storeMapResults(taskUuid, res);
		LOG.exiting(getClass().getName(), "storeMap");
	}

	void storeReduce(String taskUuid, List<String> res) {
		LOG.entering(getClass().getName(), "storeReduce", new Object[] { taskUuid });
		this.pers.storeReduceResults(taskUuid, res);
		LOG.exiting(getClass().getName(), "pushReduceResult");
	}

	@Override
	public ConcurrentMap<String, ResultState> getResultStates() {
		return this.results;
	}

}
