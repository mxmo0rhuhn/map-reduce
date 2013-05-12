package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.plugins.socket.ResultState;
import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;
import ch.zhaw.mapreduce.plugins.socket.SocketResultObserver;
import de.root1.simon.annotation.SimonRemote;

@SimonRemote(SocketResultCollector.class)
public final class SocketResultCollectorImpl implements SocketResultCollector {

	private static final Logger LOG = Logger.getLogger(SocketResultCollectorImpl.class.getName());

	private final ConcurrentMap<String, ResultState> results = new ConcurrentHashMap<String, ResultState>();

	private final Persistence pers;

	private final ExecutorService supervisorService;

	@Inject
	SocketResultCollectorImpl(Persistence pers,
			@Named("resultCollectorSuperVisorService") ExecutorService supervisorService) {
		this.pers = pers;
		this.supervisorService = supervisorService;
	}

	@PostConstruct
	public void initSupervisor() {
		LOG.entering(getClass().getName(), "initSupervisor");
		this.supervisorService.submit(createSupervisor());
		LOG.exiting(getClass().getName(), "initSupervisor");
	}

	@Override
	public void pushResult(SocketAgentResult res) {
		String mapReduceTaskUuid = res.getMapReduceTaskUuid();
		String taskUuid = res.getTaskUuid();
		LOG.entering(getClass().getName(), "pushResult", new Object[] { mapReduceTaskUuid, taskUuid });

		if (res.wasSuccessful()) {
			storeResult(res);
		} else {
			LOG.log(Level.WARNING, "Failed to run Task on Agent", res.getException());
		}
		notifySocketWorker(mapReduceTaskUuid, taskUuid, res.wasSuccessful());

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
	public Boolean registerObserver(String mapReduceTaskUuid, String taskUuid, SocketResultObserver observer) {
		LOG.entering(getClass().getName(), "registerObserver", new Object[] { mapReduceTaskUuid, taskUuid, observer });
		String key = createKey(mapReduceTaskUuid, taskUuid);
		ResultState newResultState = ResultState.requestedBy(observer);
		ResultState resultState = this.results.putIfAbsent(key, newResultState);
		Boolean result = null;
		if (resultState != null) {
			if (resultState.available()) {
				LOG.log(Level.FINE, "Result is already available for MapReduceTaskUuid={0}, TaskUuid={1}, Success={2}",
						new Object[] { mapReduceTaskUuid, taskUuid, resultState.successful() });
				this.results.remove(key);
				result = Boolean.valueOf(resultState.successful());
				LOG.log(Level.FINE,
						"Notified Observer and Removed Result from List for MapReduceTaskUuid={0}, TaskUuid={1}, Observer={2}",
						new Object[] { mapReduceTaskUuid, taskUuid, observer });
			} else if (resultState.requested()) {
				LOG.log(Level.WARNING,
						"Second Observer for same Result, MapReduceTaskUuid={0}, TaskUuid={1}, Replacing Observer={2} with Observer={3}",
						new Object[] { mapReduceTaskUuid, taskUuid, resultState.requestedBy(),
								newResultState.requestedBy() });
				this.results.put(key, newResultState);
			} else {
				throw new IllegalStateException("ResultState not handled: " + resultState);
			}
		} else {
			LOG.log(Level.FINE, "Added Observer for MapReduceTaskUuid={0}, TaskUuid={1}, Observer={2}", new Object[] {
					mapReduceTaskUuid, taskUuid, observer });
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
	private void notifySocketWorker(String mapReduceTaskUuid, String taskUuid, boolean wasSuccessful) {
		LOG.entering(getClass().getName(), "notifySocketWorker", new Object[] { mapReduceTaskUuid, taskUuid,
				wasSuccessful });
		String key = createKey(mapReduceTaskUuid, taskUuid);
		ResultState newState = ResultState.resultAvailable(wasSuccessful);
		ResultState resultState = this.results.putIfAbsent(key, newState);
		if (null != resultState) {
			if (resultState.requested()) {
				LOG.log(Level.FINE, "Result was Requested MapReduceTaskUuid={0}, TaskUuid={1}", new Object[] {
						mapReduceTaskUuid, taskUuid });
				SocketResultObserver observer = resultState.requestedBy();
				observer.resultAvailable(mapReduceTaskUuid, taskUuid, wasSuccessful);
			} else {
				this.results.put(key, newState);
				LOG.log(Level.WARNING,
						"Result was already Available. Replacing old State with new one for MapReduceTaskUuid={0}, TaskUuid={1}",
						new Object[] { mapReduceTaskUuid, taskUuid });
			}
			LOG.log(Level.FINE, "Removing Result from List for MapReduceTaskUuid={0}, TaskUuid={1}", new Object[] {
					mapReduceTaskUuid, taskUuid });
			this.results.remove(key);
		} else {
			LOG.log(Level.FINE, "Result for MapReduceTaskUuid={0}, TaskUuid={1} now Available", new Object[] {
					mapReduceTaskUuid, taskUuid });
		}
		LOG.exiting(getClass().getName(), "notifySocketWorker");
	}

	private String createKey(String mapReduceTaskUuid, String taskUuid) {
		return mapReduceTaskUuid + '-' + taskUuid;
	}

	private void storeResult(SocketAgentResult saRes) {
		List<?> res = saRes.getResult();
		if (res.isEmpty()) {
			LOG.info("Empy Result from SocketAgent");
		} else if (hasType(res, KeyValuePair.class)) {
			storeMap(saRes.getMapReduceTaskUuid(), saRes.getTaskUuid(), (List<KeyValuePair>) res);
		} else if (hasType(res, String.class)) {
			storeReduce(saRes.getMapReduceTaskUuid(), saRes.getTaskUuid(), (List<String>) res);
		} else {
			LOG.severe("Null Result from SocketAgent");
		}
	}

	/**
	 * Prüft, ob der Inhalt der Liste diesen Typ hat
	 */
	boolean hasType(List<?> res, Class<?> klass) {
		return res != null && res.get(0).getClass().equals(klass);
	}

	void storeMap(String mapReduceTaskUuid, String taskUuid, List<KeyValuePair> res) {
		LOG.entering(getClass().getName(), "storeMap", new Object[] { mapReduceTaskUuid, taskUuid });
		for (KeyValuePair pair : res) {
			String key = (String) pair.getKey();
			String val = (String) pair.getValue();
			this.pers.storeMap(mapReduceTaskUuid, taskUuid, key, val);
		}
		LOG.exiting(getClass().getName(), "storeMap");
	}

	void storeReduce(String mapReduceTaskUuid, String taskUuid, List<String> res) {
		LOG.entering(getClass().getName(), "storeReduce", new Object[] { mapReduceTaskUuid, taskUuid });
		for (String val : res) {
			this.pers.storeReduce(mapReduceTaskUuid, taskUuid, val);
		}
		LOG.exiting(getClass().getName(), "pushReduceResult");
	}

	@Override
	public List<String> getReduceResult(String mapReduceTaskUuid, String taskUuid) {
		return this.pers.getReduce(mapReduceTaskUuid, taskUuid);
	}

	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUuid, String taskUuid) {
		return this.pers.getMap(mapReduceTaskUuid, taskUuid);
	}

	@Override
	public void cleanAllResults(String mapReduceTaskUuid) {
		throw new UnsupportedOperationException("missing feature in persistence");
	}

	@Override
	public void cleanResult(String mapReduceTaskUuid, String taskUuid) {
		this.pers.destroy(mapReduceTaskUuid, taskUuid);
	}

	private Runnable createSupervisor() {
		return new Runnable() {

			@Override
			public void run() {
				try {
					while (true) {
						int size = results.size();
						LOG.log(Level.INFO, "Number of Results in List = {0}", size);
						Thread.sleep(10000);
					}
				} catch (InterruptedException ie) {
					LOG.info("Supervisor Interrupted. Stopping");
				}
			}
		};
	}

}
