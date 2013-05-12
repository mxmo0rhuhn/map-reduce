package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

import com.google.inject.assistedinject.Assisted;

/**
 * SocketWorker ist der serverseitige Worker, der die WorkerTasks zu AgentTasks konvertieren lässt und sie dann auf dem
 * SocketAgent ausführt.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class SocketWorker implements Worker, SocketResultObserver {

	private static final Logger LOG = Logger.getLogger(SocketWorker.class.getName());

	/** RPC */
	private final SocketAgent agent;

	/** Cache IP wegen Performance */
	private final String agentIP;

	private final ExecutorService taskRunnerService;

	private final Pool pool;

	private final AgentTaskFactory atFactory;

	private final SocketResultCollector resCollector;

	/**
	 * Dieser Task wird momentan auf dem SocketAgent ausgeführt. Wir behalten hier die Referenz für das Status Update,
	 * wenn das Resultat verfügbar ist.
	 */
	private volatile Future<WorkerTask> currentTask;

	/**
	 * Maxmale Zeit, die gewartet wird, um einen Task auf dem Agent auszufuehren.
	 */
	private final long agentTaskTriggeringTimeout;

	@Inject
	public SocketWorker(@Assisted SocketAgent agent, @Named("taskrunnerservice") ExecutorService exec, Pool pool,
			AgentTaskFactory atFactory, @Assisted SocketResultCollector resCollector,
			@Named("agentTaskTriggeringTimeout") long agentTaskTriggeringTimeout) {
		this.agent = agent;
		this.agentIP = agent.getIp();
		this.taskRunnerService = exec;
		this.pool = pool;
		this.atFactory = atFactory;
		this.resCollector = resCollector;
		this.agentTaskTriggeringTimeout = agentTaskTriggeringTimeout;
	}

	/**
	 * Wir nehmen an, da ein SocketWorker nur einen Task gleichzeitig ausführen kann, dass diese Notifizierung für diese
	 * Berechnung zu dem Task gehört, der gerade ausgefürt wird (currentTask). Dann setzten wir den Status vom Task und
	 * geben den SocketWorker zurück zum Pool.
	 */
	@Override
	public void resultAvailable(String mapReduceTaskUuid, String taskUuid, boolean success) {
		LOG.entering(getClass().getName(), "resultAvailable", new Object[] { mapReduceTaskUuid, taskUuid, success });
		Future<WorkerTask> future = this.currentTask;
		if (future == null) {
			LOG.log(Level.WARNING, "Got notified for missing Task {0} {1}", new Object[] { mapReduceTaskUuid, taskUuid });
			return;
		}
		try {
			WorkerTask task = future.get(agentTaskTriggeringTimeout, TimeUnit.MILLISECONDS);
			if (mapReduceTaskUuid.equals(task.getMapReduceTaskUuid()) && taskUuid.equals(task.getTaskUuid())) {
				LOG.log(Level.FINE, "Go notified for MapReduceTaskUuid={0} TaskUuid={1} Success={2}", new Object[] {
						mapReduceTaskUuid, taskUuid, success });
				if (success) {
					task.completed();
				} else {
					task.failed();
				}
			} else {
				LOG.log(Level.SEVERE,
						"Got notification for wrong WorkerTask MapReduceTaskUuid [{0} vs {1}] TaskUuid [{2} vs {3}]",
						new Object[] { mapReduceTaskUuid, task.getMapReduceTaskUuid(), taskUuid, task.getTaskUuid() });
			}
			this.currentTask = null;
			pool.workerIsFinished(SocketWorker.this);
		} catch (TimeoutException e) {
			LOG.log(Level.SEVERE, "Caught TimeoutException for MapReduceTaskUuid={0}, TaskUuid={1}", new Object[] {
					mapReduceTaskUuid, taskUuid });
			// TODO der muesste dann natuerlich wieder in pool oder so..
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Caught Exception", e);
			// TODO der muesste dann natuerlich wieder in pool oder so..
		}
		LOG.exiting(getClass().getName(), "resultAvailable");
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
	public void executeTask(final WorkerTask workerTask) {
		LOG.entering(getClass().getName(), "executeTask", workerTask);
		// TODO currentTask wird nich zurückgesetzt, wenn das resultat verfügbar ist, bevor der task komplett dem worker
		// übergeben wurde. oder wenn dieser worker gekillt wurde.
		// if (this.currentTask != null) {
		// LOG.severe("SocketWorker is already Running a Task!");
		// workerTask.failed();
		// return;
		// }
		this.currentTask = this.taskRunnerService.submit(new Callable<WorkerTask>() {

			@Override
			public WorkerTask call() throws Exception {
				String mapReduceTaskUuid = workerTask.getMapReduceTaskUuid();
				String taskUuid = workerTask.getTaskUuid();
				AgentTask agentTask = atFactory.createAgentTask(workerTask);
				LOG.log(Level.FINE, "Before running Task on Agent MapReduceTaskUuid={0}, TaskUuid={1}, Agent={2}",
						new Object[] { mapReduceTaskUuid, taskUuid, agentIP });
				AgentTaskState state = agent.runTask(agentTask); // RPC
				LOG.log(Level.FINE, "After running Task on Agent MapReduceTaskUuid={0}, TaskUuid={1}, Agent={2}",
						new Object[] { mapReduceTaskUuid, taskUuid, agentIP });
				switch (state.state()) {
				case ACCEPTED:
					LOG.log(Level.FINE, "Task Accepted by SocketAgent for MapReduceTaskUuid={0}, TaskUuid={1}",
							new Object[] { mapReduceTaskUuid, taskUuid });
					workerTask.started();
					Boolean success = resCollector.registerObserver(mapReduceTaskUuid, taskUuid, SocketWorker.this);
					if (success != null) {
						LOG.log(Level.FINE,
								"Result already available for MapReduceTaskUuid={0}, TaskUuid={1}, Success={2}",
								new Object[] { mapReduceTaskUuid, taskUuid, success });
						if (success) {
							workerTask.completed();
						} else {
							workerTask.failed();
						}
						pool.workerIsFinished(SocketWorker.this);
						// TODO gefährlich und wahrscheinlich falsch. unklar definierter zeitpunkt um in den pool zurück
						// zu gehen. currentTask müsste auch auf null gesetzt werden!
					} else {
						LOG.log(Level.FINE,
								"Result not available. Registered as Observer for MapReduceTaskUuid={0}, TaskUuid={1}",
								new Object[] { mapReduceTaskUuid, taskUuid });
					}
					break;
				case REJECTED:
					LOG.warning("Task Rejected by SocketAgent: " + state.msg());
					workerTask.failed();
					pool.workerIsFinished(SocketWorker.this);
					break;
				default:
					throw new IllegalStateException("Unhandled: " + state.state());
				}
				return workerTask;
			}
		});
		LOG.exiting(getClass().getName(), "executeTask");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> getReduceResult(String mapReduceTaskUuid, String taskUuid) {
		return this.resCollector.getReduceResult(mapReduceTaskUuid, taskUuid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUuid, String taskUuid) {
		return this.resCollector.getMapResult(mapReduceTaskUuid, taskUuid);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cleanAllResults(String mapReduceTaskUuid) {
		this.resCollector.cleanAllResults(mapReduceTaskUuid);
	}

	@Override
	public void cleanSpecificResult(String mapReduceTaskUuid, String taskUuid) {
		this.resCollector.cleanResult(mapReduceTaskUuid, taskUuid);
	}

	/**
	 * Stop den Task, der momentan grad ausgeführt wird, sodass der SocketWorker wieder für neues verfügbar ist.
	 */
	@Override
	public void stopCurrentTask(String mapReduceUuid, String taskUuid) {
		LOG.log(Level.FINE, "SocketWorker does not stop but just make itself available again");
		// TODO besser machen
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for SocketAgent with IP=" + this.agentIP;
	}

}
