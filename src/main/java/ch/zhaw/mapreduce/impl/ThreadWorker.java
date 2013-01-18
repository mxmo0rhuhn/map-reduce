package ch.zhaw.mapreduce.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

	private final ConcurrentMap<String, List<KeyValuePair>> mapResults = new ConcurrentHashMap<String, List<KeyValuePair>>();
	
	private final ConcurrentMap<String, List<KeyValuePair>> reduceResults = new ConcurrentHashMap<String, List<KeyValuePair>>();

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
		mapResults.putIfAbsent(task.getMapReduceTaskUUID(), new LinkedList<KeyValuePair>());
		reduceResults.putIfAbsent(task.getMapReduceTaskUUID(), new LinkedList<KeyValuePair>());
		
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
		List<KeyValuePair> pairs = mapResults.get(mapReduceTaskUID);
		if (pairs != null) {
			pairs.add(pair);
		} else {
			throw new IllegalStateException("MapResults not initialized for: " + mapReduceTaskUID);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void storeReduceResult(String mapReduceTaskUID, KeyValuePair pair) {
		List<KeyValuePair> pairs = reduceResults.get(mapReduceTaskUID);
		if (pairs != null) {
			pairs.add(pair);
		} else {
			throw new IllegalStateException("ReduceResults not initialized for: " + mapReduceTaskUID);
		}
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

	/** {@inheritDoc} */
	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		this.mapResults.remove(mapReduceTaskUUID);
		this.reduceResults.remove(mapReduceTaskUUID);
	}
	
	/** {@inheritDoc} */
	@Override
	public void replaceMapResult(String mapReduceTaskUID, List<KeyValuePair> newResult) {
		this.mapResults.put(mapReduceTaskUID, newResult);
	}
	
}
