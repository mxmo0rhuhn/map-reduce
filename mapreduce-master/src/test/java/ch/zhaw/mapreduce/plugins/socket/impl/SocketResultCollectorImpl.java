package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.plugins.socket.ResultCollectorObserver;
import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;
import de.root1.simon.annotation.SimonRemote;

@SimonRemote(SocketResultCollector.class)
@Singleton
public final class SocketResultCollectorImpl implements SocketResultCollector {

	private static final Logger LOG = Logger.getLogger(SocketResultCollectorImpl.class.getName());

	private final Persistence pers;

	@Inject
	SocketResultCollectorImpl(Persistence pers) {
		this.pers = pers;
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
	

	@Override
	public void registerObserver(String mapReduceTaskUuid, String taskUuid, ResultCollectorObserver observer) {
		foo bar
	}

	private void notifySocketWorker(String mapReduceTaskUuid, String taskUuid, boolean wasSuccessful) {
		asdf
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

}
