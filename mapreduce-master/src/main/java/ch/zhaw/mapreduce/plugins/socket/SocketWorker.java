package ch.zhaw.mapreduce.plugins.socket;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

import com.google.inject.assistedinject.Assisted;

/**
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class SocketWorker implements Worker {

	private static final Logger LOG = Logger.getLogger(SocketWorker.class.getName());

	private final Map<String, Future<Void>> runningTasks = Collections
			.synchronizedMap(new WeakHashMap<String, Future<Void>>());

	private final SocketAgent agent;

	private final ExecutorService exec;

	private final Persistence persistence;

	private final Pool pool;

	@Inject
	SocketWorker(@Assisted SocketAgent agent, @Named("socket.workerexecutorservice") ExecutorService exec,
			Persistence persistence, Pool pool) {
		this.agent = agent;
		this.exec = exec;
		this.persistence = persistence;
		this.pool = pool;
	}

	/**
	 * Verbindet sich über IP & Port mit Agent, sendet Instruktionen und Input.
	 * 
	 * Abfolge:
	 * <ul>
	 * <li>Sich selbst als thread starten => analog ThreadWorker</li>
	 * <li>Aufgabe an Agent senden</li>
	 * <li>Agent sendet ergebnis</li>
	 * <li>Ergebnis in eine lokale persistenz ablegen</li>
	 * <li>Worker Task auf erledigt setzen</li>
	 * </ul>
	 * 
	 * @see ch.zhaw.mapreduce.workers.Worker#executeTask(ch.zhaw.mapreduce.WorkerTask)
	 */
	@Override
	public void executeTask(final WorkerTask task) {
		Future<Void> runningTask = this.exec.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				Object result = agent.runTask(task);
				if (result == null) {
					LOG.severe("Got no result from Client");
					task.failed();
				}
				// TODO grusig
				else if (task instanceof MapWorkerTask) {
					List<KeyValuePair> mapres = (List<KeyValuePair>) result;
					// TODO grad nochmal grusig
					for (KeyValuePair pair : mapres) {
						persistence.storeMap(task.getMapReduceTaskUuid(), task.getTaskUuid(), (String) pair.getKey(),
								(String) pair.getValue());
					}
					task.completed();
				}

				else if (task instanceof ReduceWorkerTask) {
					List<String> redres = (List<String>) result;
					for (String res : redres) {
						persistence.storeReduce(task.getMapReduceTaskUuid(), task.getTaskUuid(), res);
					}
					task.completed();
				}

				else {
					throw new IllegalStateException("Task muss entweder Map oder Reduce sein");
				}
				pool.workerIsFinished(SocketWorker.this);
				return null;
			}
		});
		String combinedId = task.getMapReduceTaskUuid() + task.getTaskUuid();
		this.runningTasks.put(combinedId, runningTask);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getReduceResult(String mapReduceTaskUuid, String inputUuid) {
		return this.persistence.getReduce(mapReduceTaskUuid, inputUuid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUuid, String inputUuid) {
		return this.persistence.getMap(mapReduceTaskUuid, inputUuid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		throw new UnsupportedOperationException("missing feature in persistence");
	}

	@Override
	public void cleanSpecificResult(String mapReduceTaskUuid, String inputUuid) {
		this.persistence.destroy(mapReduceTaskUuid, inputUuid);
	}

	@Override
	public void stopCurrentTask(String mapReduceUuid, String taskUuid) {
		String combinedId = mapReduceUuid + taskUuid;
		Future<Void> runningTask = this.runningTasks.get(combinedId);
		if (runningTask != null) {
			if (runningTask.cancel(true)) {
				// task wurde gestoppt. worker muss zurueck in pool
				LOG.fine("Task gestoppt");
				this.pool.workerIsFinished(this);
			} else {
				// task konnte nicht gestoppt werden (typischerweise war er halt schon fertig). worker ist bereits
				// zurueck im pool
				LOG.fine("Task konnte nicht gestoppt werden. War schon fertig?");
				this.persistence.destroy(mapReduceUuid, taskUuid);
			}
		} else {
			LOG.info("Task nicht gefunden");
		}
	}

	SocketAgent getSocketAgent() {
		return this.agent;
	}

}
