package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

	private volatile State state = State.AVAILABLE;

	private volatile RunningTask runningTask;

	/**
	 * Maxmale Zeit, die gewartet wird, um einen Task auf dem Agent auszufuehren.
	 */
	private final long agentTaskTriggeringTimeout;

	@Inject
	public SocketWorker(@Assisted SocketAgent agent,
			@Named("SocketWorkerTaskTriggerService") ExecutorService taskRunnerService, Pool pool,
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
					if (state == State.NEW_TASK || state == State.RUNNING) {
						try {
							RunningTask runningTask = SocketWorker.this.runningTask;
							if (runningTask != null) {
								WorkerTask currentTask = runningTask.task;
								if (currentTask != null) {
									currentTask.fail();
								}
							}
						} catch (Exception e1) {
							LOG.log(Level.WARNING, "Failed to retrieve currently running Task", e);
						}
					}
					state = State.DEAD;
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
		RunningTask runningTask = this.runningTask;
		if (runningTask == null) {
			LOG.log(Level.WARNING, "Got notified but SocketWorker is not running a Task on the Agent. TaskUuid = {0}",
					new Object[] { taskUuid });
			return;
		}
		try {
			WorkerTask task = runningTask.task; // task ist nie null
			Future<Void> triggerer = runningTask.triggerer;
			if (triggerer != null) {
				triggerer.get(agentTaskTriggeringTimeout, TimeUnit.MILLISECONDS);
			}
			if (taskUuid.equals(task.getTaskUuid())) {
				LOG.log(Level.FINE, "Go notified for TaskUuid={0} Result={1}", new Object[] { taskUuid, result });
				if (result.wasSuccessful()) {
					task.successful(result.getResult());
				} else {
					LOG.log(Level.WARNING, "Task has failed on Agent", result.getException());
					task.fail();
				}
			} else {
				LOG.log(Level.SEVERE, "Got notification for wrong WorkerTask TaskUuid [{0} vs {1}]", new Object[] {
						taskUuid, task.getTaskUuid() });
			}
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Caught Exception", e);
		}
		// state muss zuerst auf available gesetzt werde, sonst wird er den naechsten task nicht akzeptieren, falls
		// dieser zu schnell kommt.
		state = State.AVAILABLE;
		if (!pool.workerIsFinished(SocketWorker.this)) {
			state = State.DEAD;
			LOG.info("Not Accepted by Pool. Probably died in the meantime");
		} else {
			LOG.fine("Went back to Pool");
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
		LOG.entering(getClass().getName(), "executeTask", new Object[] { workerTask });
		if (this.state != State.AVAILABLE) {
			LOG.warning("Cannot execture two tasks at the same time!");
			workerTask.fail();
			return;
		}
		this.state = State.NEW_TASK;

		/*
		 * Reihenfolge hier ist wichtig! Den WorkerTask brauchen wir auf jeden Fall in der Struktur (wird per volatile
		 * propagiert), weil dieser verfügbar sein muss, wenn das Resultat eintrifft, bevor die submit Methode fertig
		 * ist. Zuerst wird die Variable lokal erstellt um NullPointer zu vermeiden, falls sie in der Zwischenzeit auf
		 * null gesetzt wird von einem anderen Thread.
		 */
		RunningTask runningTask = new RunningTask(workerTask);
		this.runningTask = runningTask;
		runningTask.triggerer = this.taskRunnerService.submit(newTaskTriggerer(workerTask));
		LOG.exiting(getClass().getName(), "executeTask");
	}

	private Callable<Void> newTaskTriggerer(final WorkerTask workerTask) {
		return new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				LOG.entering(getClass().getName(), "call");
				String taskUuid = workerTask.getTaskUuid();
				AgentTask agentTask = atFactory.createAgentTask(workerTask);
				LOG.log(Level.FINE, "Before running Task on Agent TaskUuid={0}, Agent={1}", new Object[] { taskUuid,
						agentIP });
				AgentTaskState taskState = agent.runTask(agentTask); // RPC
				LOG.log(Level.FINE, "After running Task on Agent TaskUuid={0}, Agent={1}", new Object[] { taskUuid,
						agentIP });

				switch (taskState.state()) {
				case ACCEPTED:
					LOG.log(Level.FINE, "Task Accepted by SocketAgent for TaskUuid={0}", new Object[] { taskUuid });
					workerTask.started();
					SocketAgentResult result = resCollector.registerObserver(taskUuid, SocketWorker.this);
					if (result != null) {
						LOG.log(Level.FINE, "Result already available for TaskUuid={0}, Result={1}", new Object[] {
								taskUuid, result });
						if (result.wasSuccessful()) {
							workerTask.successful(result.getResult());
						} else {
							LOG.log(Level.WARNING, "Task Failed on Agent", result.getException());
							workerTask.fail();
						}

						// state muss zuerst auf available gesetzt werde, sonst wird er den naechsten task nicht
						// akzeptieren, falls dieser zu schnell kommt.
						state = State.AVAILABLE;
						if (!pool.workerIsFinished(SocketWorker.this)) {
							state = State.DEAD;
							LOG.info("Not Accepted by Pool. Probably died in the meantime");
						} else {
							LOG.info("Went back to Pool");
						}
					} else {
						LOG.log(Level.FINE, "Result not available. Registered as Observer for TaskUuid={0}",
								new Object[] { taskUuid });
					}
					break;
				case REJECTED:
					LOG.warning("Task Rejected by SocketAgent: " + taskState.msg());
					workerTask.fail();
					state = State.AVAILABLE;
					if (!pool.workerIsFinished(SocketWorker.this)) {
						state = State.DEAD;
						LOG.info("Not Accepted by Pool. Probably died in the meantime");
					} else {
						LOG.fine("Went back to Pool");
					}
					break;
				default:
					throw new IllegalStateException("Unhandled: " + taskState.state());
				}
				LOG.exiting(getClass().getName(), "call", workerTask);
				return null;
			}
		};
	}

	/**
	 * Stop den Task, der momentan grad ausgeführt wird, sodass der SocketWorker wieder für neues verfügbar ist.
	 */
	@Override
	public void stopCurrentTask(String taskUuid) {
		throw new UnsupportedOperationException("implement me");
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for SocketAgent with IP=" + this.agentIP;
	}

	private enum State {
		AVAILABLE, // verfügbar für neue Aufgaben
		NEW_TASK, // task von pool bekommen, kurz vor dem ausführen
		RUNNING, // task wird soeben auf dem agent ausgeführt
		DEAD // agent ist gestorben :(
	}

	private static class RunningTask {
		final WorkerTask task;
		Future<Void> triggerer;

		RunningTask(WorkerTask task) {
			if (task == null) {
				throw new IllegalArgumentException("Task must not be null!");
			}
			this.task = task;
		}
	}

}
