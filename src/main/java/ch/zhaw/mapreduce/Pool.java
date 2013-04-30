package ch.zhaw.mapreduce;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.WorkerTask.State;
import ch.zhaw.mapreduce.registry.PoolExecutor;

/**
 * Implementation des Pools mit lokalen Threads auf dem jeweiligen PC
 * 
 * @author Max, Desiree Sacher
 * 
 */
@Singleton
public final class Pool {

	private static final Logger LOG = Logger.getLogger(Pool.class.getName());

	private final List<Worker> existingWorkers = new CopyOnWriteArrayList<Worker>();

	// Liste mit allen Workern
	private final Queue<Worker> workingWorker = new ConcurrentLinkedQueue<Worker>();

	// Liste mit allen Workern, die Arbeit übernehmen können.
	private final BlockingQueue<Worker> availableWorkerBlockingQueue = new LinkedBlockingQueue<Worker>();

	// Liste mit aller Arbeit, die von Workern übernommen werden kann.
	private final BlockingQueue<WorkerTask> taskQueue = new LinkedBlockingQueue<WorkerTask>();

	private final AtomicBoolean isRunning = new AtomicBoolean();

	private final Executor workTaskAdministrator;

	/**
	 * Erstellt einen neuen Pool der Aufgaben und Worker entgegen nimmt.
	 */
	@Inject
	public Pool(@PoolExecutor Executor exec) {
		this.workTaskAdministrator = exec;
	}

	/**
	 * Startet den Thread zur asynchronen Arbeit
	 */
	// wird nach dem konstruktor aufgerufen
	@PostConstruct
	public void init() {
		// nur starten, wenn er noch nicht gestartet wurde
		if (this.isRunning.compareAndSet(false, true)) {
			this.workTaskAdministrator.execute(new WorkerTaskAdministrator());
			LOG.info("Pool started");
		} else {
			throw new IllegalStateException("Cannot start Pool twice");
		}
	}

	public boolean isRunning() {
		return this.isRunning.get();
	}

	/**
	 * {@inheritDoc} Der Wert ist optimistisch - kann veraltet sein.
	 */
	public int getCurrentPoolSize() {
		return existingWorkers.size();
	}

	/**
	 * {@inheritDoc}
	 */
	public int getFreeWorkers() {
		return availableWorkerBlockingQueue.size();
	}

	/**
	 * {@inheritDoc}
	 */
	public void workerIsFinished(Worker finishedWorker) {
		LOG.finest("Put Worker back to queue");
		if (!workingWorker.remove(finishedWorker)) {
			LOG.warning("Worker was not working before");
		}
		availableWorkerBlockingQueue.add(finishedWorker);
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean enqueueWork(WorkerTask task) {
		LOG.finest("Enqueue Task");
		//TODO Reto: Konzept erarbeiten 
//		task.setState(State.ENQUEUED);
		return taskQueue.offer(task);
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean donateWorker(Worker newWorker) {
		LOG.finest("Donate Worker");
		this.existingWorkers.add(newWorker);
		return availableWorkerBlockingQueue.offer(newWorker);
	}

	private class WorkerTaskAdministrator implements Runnable {

		/**
		 * Wartet auf Auftraege und fuert diese mit den Workers aus.
		 */
		@Override
		public void run() {
			try {
				while (true) {
					LOG.finest("Take Task and Worker");
					WorkerTask task = taskQueue.take(); // blockiert bis ein Task da ist
					Worker worker = availableWorkerBlockingQueue.take(); // blockiert, bis ein Worker frei ist
					workingWorker.add(worker);
					task.setWorker(worker);
					LOG.finest("Execute Task on Worker");
					worker.executeTask(task);
				}
			} catch (InterruptedException e) {
				LOG.info("Interrupted, stopping WorkerTaskAdministrator");
				isRunning.set(false);
				Thread.currentThread().interrupt();
			}
		}

	}
	
	public void computationStopped(String mapReduceTaskUUID) {
		// TODO
	}

	/** {@inheritDoc} */
	public void cleanResults(String mapReduceTaskUUID) {
		Worker[] allWorkers = this.existingWorkers.toArray(new Worker[this.existingWorkers.size()]);
		for (Worker worker : allWorkers) {
			worker.cleanAllResults(mapReduceTaskUUID);
		}
	}
}
