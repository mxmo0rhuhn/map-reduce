package ch.zhaw.mapreduce;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * @author Max, Sacher
 * 
 */
@Singleton
public final class Pool {

	private static final Logger LOG = Logger.getLogger(Pool.class.getName());

	// Liste mit allen Workern
	private final Queue<Worker> workingWorkers = new ConcurrentLinkedQueue<Worker>();

	// Liste mit allen Workern, die Arbeit übernehmen können.
	private final BlockingQueue<Worker> availableWorkers = new LinkedBlockingQueue<Worker>();

	// Liste mit aller Arbeit, die von Workern übernommen werden kann.
	private final BlockingQueue<WorkerTask> taskQueue = new LinkedBlockingQueue<WorkerTask>();

	private final AtomicBoolean isRunning = new AtomicBoolean();
	private final Executor workTaskAdministrator;
	private final ScheduledExecutorService supervisorService;
	private final Runtime runtime;
	private final long statisticsPrintTimeout;

	private final long memoryFullSleepTime;

	private final long minRemainingMemory;

	/**
	 * Erstellt einen neuen Pool der Aufgaben und Worker entgegen nimmt.
	 */
	@Inject
	public Pool(@Named("poolExecutor") Executor workTaskAdministrator,
			@Named("memoryFullSleepTime") long memoryFullSleepTime,
			@Named("minRemainingMemory") long minRemainingMemory,
			@Named("supervisorScheduler") ScheduledExecutorService supervisorService,
			@Named("statisticsPrinterTimeout") long statisticsTimeout) {
		this.workTaskAdministrator = workTaskAdministrator;
		this.supervisorService = supervisorService;
		this.statisticsPrintTimeout = statisticsTimeout;
		this.runtime = Runtime.getRuntime();
		this.memoryFullSleepTime = memoryFullSleepTime;
		this.minRemainingMemory = minRemainingMemory;
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
		this.supervisorService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				LOG.log(Level.INFO, "Pool: Worker: {0} known Worker, {1} free worker, {2} tasks",
						new Object[] { getCurrentPoolSize(), getFreeWorkers(), taskQueue.size() });
				LOG.log(Level.INFO,
						"Memory: {0}% free, consumed memory: {1} MB, free memory: {2} MB, max. memory {3} MB",
						new Object[] { calculateFreeRAM(), runtime.totalMemory() / 1024 / 1024,
								runtime.freeMemory() / 1024 / 1024,
								runtime.maxMemory() / 1024 / 1024 });
			}
		}, statisticsPrintTimeout, statisticsPrintTimeout, TimeUnit.MILLISECONDS);
	}

	public boolean isRunning() {
		return this.isRunning.get();
	}

	/**
	 * {@inheritDoc} Der Wert ist optimistisch - kann veraltet sein.
	 */
	public int getCurrentPoolSize() {
		return workingWorkers.size() + availableWorkers.size();
	}

	/**
	 * {@inheritDoc}
	 */
	public int getFreeWorkers() {
		return availableWorkers.size();
	}

	/**
	 * Diese Methode wird augerufen, wenn ein Worker nicht mehr zum Ausführen von Tasks zur
	 * Verfügung stehen soll.
	 */
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
	public void workerIsFinished(Worker finishedWorker) {
		LOG.entering(getClass().getName(), "workerIsFinished", finishedWorker);
		if (!workingWorkers.remove(finishedWorker)) {
			LOG.warning("Worker was not working before");
		}
		availableWorkers.add(finishedWorker);
		LOG.exiting(getClass().getName(), "workerIsFinished");
	}

	/**
	 * Reiht eine Aufgabe in den Pool ein
	 * {@inheritDoc}
	 * @throws InterruptedException 
	 */
	public boolean enqueueTask(WorkerTask task) throws InterruptedException {
		LOG.entering(getClass().getName(), "enqueueTask", task);
		boolean retVal = false;

		if (calculateFreeRAM() < minRemainingMemory) {
				do {
					Thread.sleep(memoryFullSleepTime);
				} while (calculateFreeRAM() < minRemainingMemory);
		}

		// könnte loopen ... aber wenn das ding nicht in die queque kommt is eh was schief
		while (!retVal) {
			retVal = taskQueue.offer(task);
		}
		task.enqueued();

		LOG.exiting(getClass().getName(), "enqueueTask", retVal);
		return retVal;
	}

	private int calculateFreeRAM() {
		int maxMem = (int) runtime.maxMemory();
		if (maxMem == 0) {
			return 100;
		}
		int usedMem = (int) ((int) runtime.totalMemory() - runtime.freeMemory());
		
		return 100 - Math.round(((usedMem/maxMem) * 100));
	}

	/**
	 * {@inheritDoc}
	 */
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
