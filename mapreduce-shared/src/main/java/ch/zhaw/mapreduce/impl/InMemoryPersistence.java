package ch.zhaw.mapreduce.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;

/**
 * Implementierung der Persistence, welche alles lokal im Speicher verwaltet. Nicht geeignet für grosse Mengen an Daten.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class InMemoryPersistence implements Persistence {

	private static final Logger LOG = Logger.getLogger(InMemoryPersistence.class.getName());

	private final ConcurrentMap<String, ConcurrentMap<String, List<KeyValuePair>>> mapResults = new ConcurrentHashMap<String, ConcurrentMap<String, List<KeyValuePair>>>();

	private final ConcurrentMap<String, ConcurrentMap<String, List<String>>> reduceResults = new ConcurrentHashMap<String, ConcurrentMap<String, List<String>>>();

	@Inject
	InMemoryPersistence() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void storeMap(String mrUuid, String taskUuid, String key, String value) {
		ConcurrentMap<String, List<KeyValuePair>> mrResults = this.mapResults.get(mrUuid);
		if (mrResults == null) {
			ConcurrentMap<String, List<KeyValuePair>> previousMrResults; // previously associated
			mrResults = new ConcurrentHashMap<String, List<KeyValuePair>>();
			if (null != (previousMrResults = this.mapResults.putIfAbsent(mrUuid, mrResults))) {
				mrResults = previousMrResults;
			}
		}
		List<KeyValuePair> tResults = mrResults.get(taskUuid);
		if (tResults == null) {
			List<KeyValuePair> previousTaskResults;
			tResults = new CopyOnWriteArrayList<KeyValuePair>();
			if (null != (previousTaskResults = mrResults.putIfAbsent(taskUuid, tResults))) {
				tResults = previousTaskResults;
			}
		}
		tResults.add(new KeyValuePair(key, value));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void storeReduce(String mrUuid, String taskUuid, String result) {
		ConcurrentMap<String, List<String>> mrResults = this.reduceResults.get(mrUuid);
		if (mrResults == null) {
			ConcurrentMap<String, List<String>> previousMrResults; // previously associated
			mrResults = new ConcurrentHashMap<String, List<String>>();
			if (null != (previousMrResults = this.reduceResults.putIfAbsent(mrUuid, mrResults))) {
				mrResults = previousMrResults;
			}
		}
		List<String> tResults = mrResults.get(taskUuid);
		if (tResults == null) {
			List<String> previousTaskResults;
			tResults = new CopyOnWriteArrayList<String>();
			if (null != (previousTaskResults = mrResults.putIfAbsent(taskUuid, tResults))) {
				tResults = previousTaskResults;
			}
		}
		tResults.add(result);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getReduce(String mrUuid, String taskUuid) {
		ConcurrentMap<String, List<String>> mrResults = this.reduceResults.get(mrUuid);
		if (mrResults == null) {
			return Collections.emptyList();
		}
		return mrResults.get(taskUuid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMap(String mrUuid, String taskUuid) {
		ConcurrentMap<String, List<KeyValuePair>> mrResults = this.mapResults.get(mrUuid);
		if (mrResults == null) {
			return Collections.emptyList();
		}
		return mrResults.get(taskUuid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void replaceMap(String mrUuid, String taskUuid, List<KeyValuePair> afterCombining)
			throws IllegalArgumentException {
		ConcurrentMap<String, List<KeyValuePair>> mrResults = this.mapResults.get(mrUuid);
		if (mrResults != null) {
			if (null == mrResults.put(taskUuid, afterCombining)) {
				LOG.warning("Replacing Results where none existed before..");
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void destroy(String mrUuid, String taskUuid) throws IllegalArgumentException {
		ConcurrentMap<String, List<KeyValuePair>> mrMapResults = this.mapResults.get(mrUuid);
		if (mrMapResults != null) {
			mrMapResults.remove(taskUuid);
		}
		ConcurrentMap<String, List<String>> mrReduceResults = this.reduceResults.get(mrUuid);
		if (mrReduceResults != null) {
			mrReduceResults.remove(taskUuid);
		}
	}

}
