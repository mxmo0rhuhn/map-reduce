package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.registry.PoolExecutor;

/**
 * Implementation des Pools mit lokalen Threads auf dem jeweiligen PC
 * 
 * @author Max, Desiree Sacher
 * 
 */
@Singleton
public final class LocalThreadPool implements Pool {
	
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
	public LocalThreadPool(@PoolExecutor Executor exec) {
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
		} else {
			throw new IllegalStateException("Cannot start Pool twice");
		}
	}

	@Override
	public boolean isRunning() {
		return this.isRunning.get();
	}

	/**
	 * {@inheritDoc} Der Wert ist optimistisch - kann veraltet sein.
	 */
	@Override
	public int getCurrentPoolSize() {
		return existingWorkers.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getFreeWorkers() {
		return availableWorkerBlockingQueue.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void workerIsFinished(Worker finishedWorker) {
		workingWorker.remove(finishedWorker);
		availableWorkerBlockingQueue.add(finishedWorker);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean enqueueWork(WorkerTask task) {
		return taskQueue.offer(task);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean donateWorker(Worker newWorker) {
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
					WorkerTask task = taskQueue.take(); // blockiert bis ein Task da ist
					Worker worker = availableWorkerBlockingQueue.take(); // blockiert, bis ein Worker frei ist
					workingWorker.add(worker);
					worker.execute(task);
				}
			} catch (InterruptedException e) {
				isRunning.set(false);
				Thread.currentThread().interrupt();
			}
		}

	}

	/** {@inheritDoc} */
	@Override
	public void cleanResults(String mapReduceTaskUUID) {
		Worker[] allWorkers = this.existingWorkers.toArray(new Worker[0]);
		for (Worker worker : allWorkers) {
			worker.cleanAllResults(mapReduceTaskUUID);
		}
	}
}