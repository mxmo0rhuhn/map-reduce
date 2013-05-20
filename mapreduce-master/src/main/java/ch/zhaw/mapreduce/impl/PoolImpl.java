package ch.zhaw.mapreduce.impl;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

/**
 * Implementation des Pools mit lokalen Threads auf dem jeweiligen PC
 * 
 * @author Max, Sacher
 * 
 */
public final class PoolImpl implements Pool {

	private static final Logger LOG = Logger.getLogger(PoolImpl.class.getName());

	// Liste mit allen Workern
	private final Queue<Worker> workingWorkers = new ConcurrentLinkedQueue<Worker>();

	// Liste mit allen Workern, die Arbeit übernehmen können.
	private final BlockingQueue<Worker> availableWorkers = new LinkedBlockingQueue<Worker>();

	// Liste mit aller Arbeit, die von Workern übernommen werden kann.
	private final BlockingQueue<WorkerTask> taskQueue = new LinkedBlockingQueue<WorkerTask>();

	private final AtomicBoolean isRunning = new AtomicBoolean();

	private final Executor workTaskAdministrator;

	/**
	 * Erstellt einen neuen Pool der Aufgaben und Worker entgegen nimmt.
	 */
	@Inject
	public PoolImpl(@Named("poolExecutor") Executor workTaskAdministrator) {
		this.workTaskAdministrator = workTaskAdministrator;
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
	@Override
	public int getCurrentPoolSize() {
		return workingWorkers.size() + availableWorkers.size();
	}

	@Override
	public int getFreeWorkers() {
		return availableWorkers.size();
	}

	@Override
	public int enqueuedTasks() {
		return this.taskQueue.size();
	}

	/**
	 * Diese Methode wird augerufen, wenn ein Worker nicht mehr zum Ausführen von Tasks zur Verfügung stehen soll.
	 */
	@Override
	public void iDied(Worker deadWorker) {
		LOG.entering(getClass().getName(), "iDied", deadWorker);
		// wir versuchen einfach zu löschen, falls er existiert
		if (this.availableWorkers.remove(deadWorker)) {
			LOG.log(Level.INFO, "Removed {0} from availableWorkers", new Object[] { deadWorker });
		}
		if (this.workingWorkers.remove(deadWorker)) {
			LOG.log(Level.INFO, "Removed {0} from workingWorker", new Object[] { deadWorker });
		}
		LOG.exiting(getClass().getName(), "iDied");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean workerIsFinished(Worker finishedWorker) {
		LOG.entering(getClass().getName(), "workerIsFinished", finishedWorker);
		boolean accepted;
		if (!workingWorkers.remove(finishedWorker)) {
			LOG.warning("Worker was not working before");
			accepted = false;
		} else {
			availableWorkers.add(finishedWorker);
			accepted = true;
		}
		LOG.exiting(getClass().getName(), "workerIsFinished", accepted);
		return accepted;
	}

	/**
	 * Reiht eine Aufgabe in den Pool ein {@inheritDoc}
	 * 
	 * @throws InterruptedException
	 */
	@Override
	public boolean enqueueTask(WorkerTask task) {
		LOG.entering(getClass().getName(), "enqueueTask", task);
		boolean retVal = false;
		retVal = taskQueue.offer(task);
		task.enqueued();
		LOG.exiting(getClass().getName(), "enqueueTask", retVal);
		return retVal;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean donateWorker(Worker newWorker) {
		LOG.entering(getClass().getName(), "donateWorker", newWorker);
		boolean retVal = availableWorkers.offer(newWorker);
		LOG.exiting(getClass().getName(), "donateWorker", retVal);
		return retVal;
	}

	private class WorkerTaskAdministrator implements Runnable {

		/**
		 * Wartet auf Auftraege und fuert diese mit den Workers aus.
		 */
		@Override
		public void run() {
			try {
				while (true) {
					LOG.finest("Waiting for Task and Worker");
					WorkerTask task = taskQueue.take(); // blockiert bis ein Task da ist
					Worker worker = availableWorkers.take(); // blockiert, bis ein Worker frei ist
					workingWorkers.add(worker);

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
}
