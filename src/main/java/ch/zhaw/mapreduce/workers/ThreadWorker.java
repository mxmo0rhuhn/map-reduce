package ch.zhaw.mapreduce.workers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.FilePersistence;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.registry.WorkerExecutor;

/**
 * Implementation von einem Thread-basierten Worker. Der Task wird ueber einen Executor ausgefuehrt.
 * 
 * @author Reto
 * 
 */
public class ThreadWorker implements Worker {

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
	public ThreadWorker(Pool pool, @WorkerExecutor Executor executor) {
		this.pool = pool;
		this.executor = executor;
	}

	/**
	 * Fuehrt den gegebenen Task asynchron aus und offierirt sich selbst am Ende wieder dem Pool.
	 */
	@Override
	public void executeTask(final WorkerTask task) {
		String mrUuid = task.getMapReduceTaskUUID();
		String inputUuid = task.getUUID();
		// TODO guice (assisted inject)
		final Context ctx = new LocalContext(new FilePersistence(), mrUuid, inputUuid);
		contexts.putIfAbsent(mrUuid, new ConcurrentHashMap<String, Context>());
		ConcurrentMap<String, Context> inputs = contexts.get(mrUuid);
		inputs.put(inputUuid, ctx);
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				try {
					task.runTask(ctx);					
				} catch (ComputationStoppedException stopped) {
					// TODO LOG
				}
				pool.workerIsFinished(ThreadWorker.this);
			}

		});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUID, String inputUUID) {
		return this.contexts.get(mapReduceTaskUID).get(inputUUID).getMapResult();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getReduceResult(String mapReduceTaskUID, String inputUUID) {
		return this.contexts.get(mapReduceTaskUID).get(inputUUID).getMapResult();
	}

	/** {@inheritDoc} */
	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		for (Map.Entry<String, Context> inputs : this.contexts.get(mapReduceTaskUUID).entrySet()) {
			inputs.getValue().destroy();
		}
		this.contexts.remove(mapReduceTaskUUID);
	}

}

class LocalContext implements Context {

	private final Persistence persistence;

	private final String mrUuid;

	private final String inputUuid;
	
	private volatile boolean stopped = false;

	LocalContext(Persistence persistence, String mrUuid, String inputUuid) {
		this.mrUuid = mrUuid;
		this.inputUuid = inputUuid;
		this.persistence = persistence;
	}

	@Override
	public void emitIntermediateMapResult(String key, String value) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.store(mrUuid, inputUuid, key, value);
	}

	@Override
	public void emit(String result) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.store(mrUuid, inputUuid, result);
	} 

	@Override
	public List<KeyValuePair> getMapResult() {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		return persistence.get(mrUuid, inputUuid);
	}

	@Override
	public void replaceMapResult(List<KeyValuePair> afterCombining) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.replace(mrUuid, inputUuid, afterCombining);
	}

	@Override
	public void destroy() {
		stopped = true;
		persistence.destroy(mrUuid, inputUuid);
	}

}