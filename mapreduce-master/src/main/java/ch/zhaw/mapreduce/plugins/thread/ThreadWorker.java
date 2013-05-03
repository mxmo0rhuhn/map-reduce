package ch.zhaw.mapreduce.plugins.thread;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

import com.google.inject.name.Named;

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
	private final ExecutorService executor;

	private final ContextFactory ctxFactory;

	/**
	 * Moegliche Tasks, die gerade von diesem Worker ausgefuehrt werden.
	 * 
	 * @DesignReason Map: damit nicht ein task einer anderen id gekillt wird
	 * @DesignReason Weak: weil wir nicht genau wissen, wann wir aufräumen können, überlassen wir das dem GC
	 */
	private final WeakHashMap<String, Future<Void>> tasks = new WeakHashMap<String, Future<Void>>();

	/**
	 * Erstellt einen neunen ThreadWorker mit dem gegebenen Pool und Executor.
	 * 
	 * @param pool
	 * @param executor
	 */
	@Inject
	public ThreadWorker(Pool pool, @Named("ThreadWorker") ExecutorService executor, ContextFactory ctxFactory) {
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
		Future<Void> action = this.executor.submit(new Callable<Void>() {
			@Override
			public Void call() {
				task.runTask(ctx);
				// TODO das ist nur eine moegliche stelle, um den worker zurueck in den pool zu schieben. sollte das
				// zentral geloest werden?
				pool.workerIsFinished(ThreadWorker.this);
				return null;
			}
		});
		this.tasks.put(mrUuid + taskUuid, action);
	}

	@Override
	public void stopCurrentTask(String mapReduceUUID, String taskUUID) {
		Future<Void> task = this.tasks.get(mapReduceUUID + taskUUID);
		if (task != null) {
			if (task.cancel(true)) {
				// Task wurde abgebrochen
				pool.workerIsFinished(this);
			} else {
				// Task war schon beendet. Worker ist bereits zurueck im Pool
			}
			LOG.fine("Stopped Task");
		} else {
			LOG.warning("No current Task available for this MapReduceID");
		}
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