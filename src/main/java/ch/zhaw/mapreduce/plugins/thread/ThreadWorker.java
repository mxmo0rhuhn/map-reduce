package ch.zhaw.mapreduce.plugins.thread;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

/**
 * Implementation von einem Thread-basierten Worker. Der Task wird ueber einen Executor ausgefuehrt.
 * 
 * @author Reto
 * 
 */
public class ThreadWorker implements Worker {

	private static final Logger LOG = Logger.getLogger(ThreadWorker.class.getName());

	private final ConcurrentMap<String, ConcurrentMap<String, Context>> contexts = new ConcurrentHashMap<String, ConcurrentMap<String, Context>>();

	/**
	 * Aus dem Pool kommt der Worker her und dahin muss er auch wieder zurueck.
	 */
	private final Pool pool;

	/**
	 * Der Executor ist fuer asynchrone ausfuehren.
	 */
	private final Executor executor;

	private final ContextFactory ctxFactory;

	/**
	 * Erstellt einen neunen ThreadWorker mit dem gegebenen Pool und Executor.
	 * 
	 * @param pool
	 * @param executor
	 */
	@Inject
	public ThreadWorker(Pool pool, Executor executor, ContextFactory ctxFactory) {
		this.pool = pool;
		this.executor = executor;
		this.ctxFactory = ctxFactory;
	}

	/**
	 * Fuehrt den gegebenen Task asynchron aus und offierirt sich selbst am Ende wieder dem Pool.
	 */
	@Override
	public void executeTask(final WorkerTask task) {
		String mrUuid = task.getMapReduceTaskUUID();
		String taskUuid = task.getUUID();
		final Context ctx = this.ctxFactory.createContext(mrUuid, taskUuid);
		this.contexts.putIfAbsent(mrUuid, new ConcurrentHashMap<String, Context>());
		ConcurrentMap<String, Context> inputs = this.contexts.get(mrUuid);
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
	public List<KeyValuePair> getMapResult(String mapReduceTaskUUID, String mapTaskUUID) {
		ConcurrentMap<String, Context> computationCtx = this.contexts.get(mapReduceTaskUUID);
		if (computationCtx == null) {
			LOG.fine("MapReduceTaskUUID not found: " + mapReduceTaskUUID);
			return Collections.emptyList();
		}
		Context taskCtx = computationCtx.get(mapTaskUUID);
		if (taskCtx == null) {
			LOG.finer("MapTaskUUID not found: " + mapTaskUUID);
			return Collections.emptyList();
		}
		return taskCtx.getMapResult();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getReduceResult(String mapReduceTaskUUID, String reduceTaskUUID) {
		ConcurrentMap<String, Context> computationCtx = this.contexts.get(mapReduceTaskUUID);
		if (computationCtx == null) {
			LOG.fine("MapReduceTaskUUID not found: " + mapReduceTaskUUID);
			return Collections.emptyList();
		}
		Context taskCtx = computationCtx.get(reduceTaskUUID);
		if (taskCtx == null) {
			LOG.finer("ReduceTaskUUID not found: " + reduceTaskUUID);
			return Collections.emptyList();
		}
		return taskCtx.getReduceResult();
	}

	/** {@inheritDoc} */
	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		Map<String, Context> mrContexts = this.contexts.get(mapReduceTaskUUID);
		if (mrContexts == null) {
			LOG.finest("Nothing to delete for " + mapReduceTaskUUID);
		} else {
			for (Map.Entry<String, Context> inputs : mrContexts.entrySet()) {
				inputs.getValue().destroy();
			}
		}
		this.contexts.remove(mapReduceTaskUUID);
	}

	@Override
	public void cleanSpecificResult(String mapReduceTaskUUID, String taskUUID) {
		ConcurrentMap<String, Context> tasks = this.contexts.get(mapReduceTaskUUID);
		if (tasks == null) {
			LOG.finest("Nothing to clean for " + mapReduceTaskUUID);
		} else {
			Context ctx = tasks.get(taskUUID);
			if (ctx == null) {
				LOG.finest("Nothing to delete for " + mapReduceTaskUUID + " and " + taskUUID);
			} else {
				ctx.destroy();
				tasks.remove(taskUUID);
			}
		}
	}

}