package ch.zhaw.mapreduce.plugins.thread;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.ComputationStoppedException;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.registry.Registry;
import ch.zhaw.mapreduce.workers.Worker;

/**
 * Implementation von einem Thread-basierten Worker. Der Task wird ueber einen Executor ausgefuehrt.
 * 
 * @author Reto
 * 
 */
public class ThreadWorker implements Worker {

	@Inject
	private Logger logger;

	private final ConcurrentMap<String, ConcurrentMap<String, Context>> contexts = new ConcurrentHashMap<String, ConcurrentMap<String, Context>>();

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
	public ThreadWorker(Pool pool, Executor executor) {
		this.pool = pool;
		this.executor = executor;
	}

	/**
	 * Fuehrt den gegebenen Task asynchron aus und offierirt sich selbst am Ende wieder dem Pool.
	 */
	@Override
	public void executeTask(final WorkerTask task) {
		String mrUuid = task.getMapReduceTaskUUID();
		String taskUuid = task.getUUID();
		// TODO guice (assisted inject)
		Persistence persistence = Registry.getComponent(Persistence.class);
		final Context ctx = new LocalContext(persistence, mrUuid, taskUuid);
		contexts.putIfAbsent(mrUuid, new ConcurrentHashMap<String, Context>());
		ConcurrentMap<String, Context> inputs = contexts.get(mrUuid);
		inputs.put(taskUuid, ctx);
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				task.runTask(ctx);
				pool.workerIsFinished(ThreadWorker.this);
			}

		});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUID, String mapTaskUuid) {
		return this.contexts.get(mapReduceTaskUID).get(mapTaskUuid).getMapResult();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getReduceResult(String mapReduceTaskUID, String inputUUID) {
		return this.contexts.get(mapReduceTaskUID).get(inputUUID).getReduceResult();
	}

	/** {@inheritDoc} */
	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		Map<String, Context> mrContexts = this.contexts.get(mapReduceTaskUUID);
		if (mrContexts == null) {
			logger.finest("Nothing to delete for " + mapReduceTaskUUID);
		}
		for (Map.Entry<String, Context> inputs : mrContexts.entrySet()) {
			inputs.getValue().destroy();
		}
		this.contexts.remove(mapReduceTaskUUID);
	}

}

class LocalContext implements Context {

	private final Persistence persistence;

	private final String mrUuid;

	private final String taskUuid;

	private volatile boolean stopped = false;

	LocalContext(Persistence persistence, String mrUuid, String taskUuid) {
		this.mrUuid = mrUuid;
		this.taskUuid = taskUuid;
		this.persistence = persistence;
	}

	@Override
	public void emitIntermediateMapResult(String key, String value) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.storeMap(mrUuid, taskUuid, key, value);
	}

	@Override
	public void emit(String result) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.storeReduce(mrUuid, taskUuid, result);
	}

	@Override
	public List<KeyValuePair> getMapResult() {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		return persistence.getMap(mrUuid, taskUuid);
	}

	@Override
	public void replaceMapResult(List<KeyValuePair> afterCombining) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.replaceMap(mrUuid, taskUuid, afterCombining);
	}

	@Override
	public void destroy() {
		stopped = true;
		persistence.destroy(mrUuid, taskUuid);
	}

	@Override
	public List<String> getReduceResult() {
		return persistence.getReduce(mrUuid, taskUuid);
	}

}