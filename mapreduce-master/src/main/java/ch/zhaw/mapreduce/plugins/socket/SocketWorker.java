package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

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

	private final ScheduledExecutorService socketScheduler;

	private final long agentPingerDelay;

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
	public SocketWorker(@Assisted SocketAgent agent, @Named("SocketWorkerTaskTriggerService") ExecutorService taskRunnerService, Pool pool,
			AgentTaskFactory atFactory, @Assisted SocketResultCollector resCollector,
			@Named("AgentTaskTriggeringTimeout") long agentTaskTriggeringTimeout,
			@Named("SocketScheduler") ScheduledExecutorService socketScheduler,
			@Named("AgentPingerDelay") long agentPingerDelay) {
		this.agent = agent;
		this.agentIP = agent.getIp(); // cache to avoid rpc while logging
		this.taskRunnerService = taskRunnerService;
		this.pool = pool;
		this.atFactory = atFactory;
		this.resCollector = resCollector;
		this.agentTaskTriggeringTimeout = agentTaskTriggeringTimeout;
		this.socketScheduler = socketScheduler;
		this.agentPingerDelay = agentPingerDelay;
	}

	@PostConstruct
	public void startAgentPinger() {
		this.socketScheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				LOG.entering("SocketAgentPinger", "run", agentIP);
				try {
					agent.ping();
				} catch (Exception e) {
					pool.iDied(SocketWorker.this);
					throw new RuntimeException("SocketAgent has died. No more pinging necessary");
				}
				LOG.exiting("SocketAgentPinger", "run", agentIP);
			}
		}, this.agentPingerDelay, this.agentPingerDelay, TimeUnit.MILLISECONDS);
	}

	/**
	 * Wir nehmen an, da ein SocketWorker nur einen Task gleichzeitig ausführen kann, dass diese Notifizierung für diese
	 * Berechnung zu dem Task gehört, der gerade ausgefürt wird (currentTask). Dann setzten wir den Status vom Task und
	 * geben den SocketWorker zurück zum Pool.
	 */
	@Override
	public void resultAvailable(String taskUuid, SocketAgentResult result) {
		LOG.entering(getClass().getName(), "resultAvailable", new Object[] { taskUuid, result });
		Future<WorkerTask> future = this.currentTask;
		if (future == null) {
			LOG.log(Level.WARNING, "Got notified for missing Task {0}",
					new Object[] { taskUuid });
			return;
		}
		try {
			WorkerTask task = future.get(agentTaskTriggeringTimeout, TimeUnit.MILLISECONDS);
			if (taskUuid.equals(task.getTaskUuid())) {
				LOG.log(Level.FINE, "Go notified for TaskUuid={0} Result={1}", new Object[] {
						taskUuid, result });
				if (result.wasSuccessful()) {
					task.successful(result.getResult());
				} else {
					LOG.log(Level.WARNING, "Task has failed on Agent", result.getException());
					task.failed();
				}
				this.currentTask = null;
			} else {
				LOG.log(Level.SEVERE,
						"Got notification for wrong WorkerTask MapReduceTaskUuid [TaskUuid [{0} vs {1}]", new Object[] { taskUuid, task.getTaskUuid() });
			}
		} catch (TimeoutException e) {
			LOG.log(Level.SEVERE, "Caught TimeoutException for TaskUuid={0}", new Object[] { taskUuid });
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Caught Exception", e);
		}
		pool.workerIsFinished(SocketWorker.this);
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
		this.currentTask = this.taskRunnerService.submit(new Callable<WorkerTask>() {

			@Override
			public WorkerTask call() throws Exception {
				String taskUuid = workerTask.getTaskUuid();
				AgentTask agentTask = atFactory.createAgentTask(workerTask);
				LOG.log(Level.FINE, "Before running Task on Agent TaskUuid={0}, Agent={1}", new Object[] { taskUuid, agentIP });
				AgentTaskState state = agent.runTask(agentTask); // RPC
				LOG.log(Level.FINE, "After running Task on Agent TaskUuid={0}, Agent={1}", new Object[] { taskUuid, agentIP });
				switch (state.state()) {
				case ACCEPTED:
					LOG.log(Level.FINE, "Task Accepted by SocketAgent for TaskUuid={0}", new Object[] { taskUuid });
					workerTask.started();
					SocketAgentResult result = resCollector.registerObserver(taskUuid, SocketWorker.this);
					if (result != null) {
						LOG.log(Level.FINE, "Result already available for TaskUuid={0}, Result={1}", new Object[] { taskUuid, result });
						if (result.wasSuccessful()) {
							workerTask.successful(result.getResult());
						} else {
							LOG.log(Level.WARNING, "Task Failed on Agent", result.getException());
							workerTask.failed();
						}
						pool.workerIsFinished(SocketWorker.this);
						// TODO gefährlich und wahrscheinlich falsch. unklar definierter zeitpunkt um in den pool zurück
						// zu gehen. currentTask müsste auch auf null gesetzt werden!
					} else {
						LOG.log(Level.FINE,
								"Result not available. Registered as Observer for TaskUuid={0}",
								new Object[] { taskUuid });
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
	 * Stop den Task, der momentan grad ausgeführt wird, sodass der SocketWorker wieder für neues verfügbar ist.
	 */
	@Override
	public void stopCurrentTask(String taskUuid) {
		LOG.log(Level.FINE, "SocketWorker does not stop but just make itself available again");
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for SocketAgent with IP=" + this.agentIP;
	}

}
