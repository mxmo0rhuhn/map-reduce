package ch.zhaw.mapreduce.impl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.registry.WorkerExecutor;

/**
 * Implementation von einem Thread-basierten Worker. Der Task wird ueber einen Executor ausgefuehrt.
 * 
 * @author Reto
 * 
 */
public class ThreadWorker implements Worker {

	private final Map<String, List<KeyValuePair>> mapResults = new HashMap<String, List<KeyValuePair>>();
	
	private final Map<String, List<KeyValuePair>> reduceResults = new HashMap<String, List<KeyValuePair>>();

	/**
	 * Aus dem Pool kommt der Worker her und dahin muss er auch wieder zurueck.
	 */
	private final Pool pool;

	/**
	 * Der Executor ist fuer asynchrone ausfuehren.
	 */
	private final Executor executor;

	/**
	 * Erstellt einen neunen ThreadWorker mit dem gegebenen Pool und Executor.
	 * 
	 * @param pool
	 * @param executor
	 */
	@Inject
	public ThreadWorker(Pool pool, @WorkerExecutor Executor executor) {
		this.pool = pool;
		this.executor = executor;
	}

	/**
	 * Fuehrt den gegebenen Task asynchron aus und offierirt sich selbst am Ende wieder dem Pool.
	 */
	@Override
	public void execute(final WorkerTask task) {
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				task.doWork(ThreadWorker.this);
				pool.workerIsFinished(ThreadWorker.this);
			}

		});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void storeMapResult(String mapReduceTaskUID, KeyValuePair pair) {
		// TODO: Sollte auch save sein, wenn gerade ein anderer MapReduce Task an seine Daten will
		List<KeyValuePair> newKeyValues;

		if (mapResults.containsKey(mapReduceTaskUID)) {
			newKeyValues = mapResults.get(mapReduceTaskUID);
		} else {
			newKeyValues = new LinkedList<KeyValuePair>();
		}

		newKeyValues.add(pair);
		mapResults.put(mapReduceTaskUID, newKeyValues);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void storeReduceResult(String mapReduceTaskUID, KeyValuePair pair) {
		// TODO: Sollte auch save sein, wenn gerade ein anderer MapReduce Task an seine Daten will
		List<KeyValuePair> newKeyValues;

		if (reduceResults.containsKey(mapReduceTaskUID)) {
			newKeyValues = reduceResults.get(mapReduceTaskUID);
		} else {
			newKeyValues = new LinkedList<KeyValuePair>();
		}

		newKeyValues.add(pair);
		reduceResults.put(mapReduceTaskUID, newKeyValues);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMapResults(String mapReduceTaskUID) {
		return this.mapResults.get(mapReduceTaskUID);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getReduceResults(String mapReduceTaskUID) {
		return this.reduceResults.get(mapReduceTaskUID);
	}
}
