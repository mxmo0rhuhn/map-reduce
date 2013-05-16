package ch.zhaw.mapreduce.plugins.thread;

import java.util.List;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
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

	/**
	 * Aus dem Pool kommt der Worker her und dahin muss er auch wieder zurueck.
	 */
	private final Pool pool;

	/**
	 * Der Executor ist fuer asynchrone ausfuehren.
	 */
	private final ExecutorService executor;

	private final Provider<Context> ctxProvider;

	private final Persistence persistence;

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
	public ThreadWorker(Pool pool, @Named("ThreadWorker") ExecutorService executor, Provider<Context> ctxProvider,
			Persistence persistence) {
		this.pool = pool;
		this.executor = executor;
		this.ctxProvider = ctxProvider;
		this.persistence = persistence;
	}

	/**
	 * Fuehrt den gegebenen Task asynchron aus und offierirt sich selbst am Ende wieder dem Pool.
	 */
	@Override
	public void executeTask(final WorkerTask task) {
		final String taskUuid = task.getTaskUuid();
		Future<Void> action = this.executor.submit(new Callable<Void>() {
			@Override
			public Void call() {
				LOG.entering(getClass().getName(), "executeTask.call", task);
				try {
					Context ctx = ctxProvider.get();
					task.runTask(ctx);
					persistContext(taskUuid, ctx);
				} catch (Exception e) {
					LOG.log(Level.SEVERE, "Failed to run Task", e);
					task.failed();
				}
				pool.workerIsFinished(ThreadWorker.this);
				LOG.exiting(getClass().getName(), "executeTask.call");
				return null;
			}
		});
		this.tasks.put(taskUuid, action);
	}

	@Override
	public void stopCurrentTask(String taskUUID) {
		Future<Void> task = this.tasks.get(taskUUID);
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
	
	void persistContext(String taskUuid, Context ctx) {
		List<KeyValuePair> mapRes = ctx.getMapResult();
		if (mapRes != null) {
			persistence.storeMapResults(taskUuid, mapRes);
		}
		List<String> redRes = ctx.getReduceResult();
		if (redRes != null) {
			persistence.storeReduceResults(taskUuid, redRes);
		}
	}
}