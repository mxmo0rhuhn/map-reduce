package ch.zhaw.mapreduce;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

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

	private final ExecutorService supervisorService;
	private final Runtime runtime;
	private final long statisticsPrintTimeout;

	/**
	 * Erstellt einen neuen Pool der Aufgaben und Worker entgegen nimmt.
	 */
	@Inject
	public Pool(@Named("poolExecutor") Executor workTaskAdministrator,
			@Named("PoolSupervisor") ExecutorService supervisorService, @Named("StatisticsPrinterTimeout") long statisticsTimeout) {
		this.workTaskAdministrator = workTaskAdministrator;
		this.supervisorService = supervisorService;
		this.statisticsPrintTimeout = statisticsTimeout;
		this.runtime = Runtime.getRuntime();
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

	@PostConstruct
	public void startSupervisor() {
		this.supervisorService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						LOG.log(Level.INFO, "Statistics: {0} known Worker, {1} free worker, {2} tasks, consumed memory: {3} MB, free memory: {4} MB, max. memory {5} MB", new Object[] { getCurrentPoolSize(), getFreeWorkers(), taskQueue.size(), runtime.totalMemory()/1024/1024 , runtime.freeMemory()/1024/1024, runtime.maxMemory()/1024/1024});
					    
						Thread.sleep(statisticsPrintTimeout);
					}
				} catch (InterruptedException ie) {
					LOG.info("Pool Supervisor Interrupted. Stopping"); 
				}
			}
		});
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
	 * Diese Methode wird augerufen, wenn ein Worker nicht mehr zum Ausführen von Tasks zur Verfügung stehen soll.
	 */
	public void iDied(Worker deadWorker) {
		LOG.entering(getClass().getName(), "iDied", deadWorker);
		// wir versuchen einfach zu löschen, falls er existiert
		if (this.availableWorkerBlockingQueue.remove(deadWorker)) {
			LOG.log(Level.INFO, "Removed {0} from availableWorkers", new Object[]{deadWorker});
		}
		if (this.workingWorker.remove(deadWorker)) {
			LOG.log(Level.INFO, "Removed {0} from workingWorker", new Object[]{deadWorker});
		}
		if(this.existingWorkers.remove(deadWorker)) {
			LOG.log(Level.INFO, "Removed {0} from existingWorkers", new Object[]{deadWorker});
		}
		LOG.exiting(getClass().getName(), "iDied");
	}

	/**
	 * {@inheritDoc}
	 */
	public void workerIsFinished(Worker finishedWorker) {
		LOG.entering(getClass().getName(), "workerIsFinished", finishedWorker);
		if (!workingWorker.remove(finishedWorker)) {
			LOG.warning("Worker was not working before");
		}
		availableWorkerBlockingQueue.add(finishedWorker);
		LOG.exiting(getClass().getName(), "workerIsFinished");
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean enqueueTask(WorkerTask task) {
		LOG.entering(getClass().getName(), "enqueueTask", task);
	
//		runtime.totalMemory(), runtime.freeMemory(), runtime.maxMemory()
//		while (!retVal) {
		boolean retVal = taskQueue.offer(task);
		if(retVal) {
			task.enqueued();
//		} else {
//			wait
//		}
		}
		LOG.exiting(getClass().getName(), "enqueueTask", retVal);
		return retVal;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean donateWorker(Worker newWorker) {
		LOG.entering(getClass().getName(), "donateWorker", newWorker);
		this.existingWorkers.add(newWorker);
		boolean retVal = availableWorkerBlockingQueue.offer(newWorker);
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
					Worker worker = availableWorkerBlockingQueue.take(); // blockiert, bis ein Worker frei ist
					workingWorker.add(worker);
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

	/** {@inheritDoc} */
	public void cleanResults(String mapReduceTaskUUID) {
		LOG.entering(getClass().getName(), "cleanResults", mapReduceTaskUUID);
		Worker[] allWorkers = this.existingWorkers.toArray(new Worker[this.existingWorkers.size()]);
		for (Worker worker : allWorkers) {
			worker.cleanAllResults(mapReduceTaskUUID);
		}
		LOG.exiting(getClass().getName(), "cleanResults");
	}
}
