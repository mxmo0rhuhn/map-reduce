package ch.zhaw.mapreduce.plugins.socket.impl;

import static ch.zhaw.mapreduce.plugins.socket.AgentTaskState.State.ACCEPTED;
import static ch.zhaw.mapreduce.plugins.socket.AgentTaskState.State.REJECTED;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.plugins.socket.AgentTask;
import ch.zhaw.mapreduce.plugins.socket.AgentTaskState;
import ch.zhaw.mapreduce.plugins.socket.SocketAgent;
import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;
import ch.zhaw.mapreduce.plugins.socket.SocketAgentResultFactory;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;
import ch.zhaw.mapreduce.plugins.socket.TaskResult;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;
import ch.zhaw.mapreduce.plugins.socket.TaskRunnerFactory;

import com.google.inject.assistedinject.Assisted;

import de.root1.simon.annotation.SimonRemote;

/**
 * Der Socket Agent ist quasi der Client-Seitige Worker. Er führt Tasks aus und gibt das Resultat zurück an den Master.
 * Der SocketAdapter ist somit der verbindende Teil zwischen dem Server und Client von der Client-Seite. Er wird bei der
 * initialen Registrierung auf den Server gesandt und als Callback wird ein Task damit ausgeführt.
 * 
 * Diese Klasse muss die Annotation '@SimonRemote' haben, weil sie über den Socket geschickt wird.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
@SimonRemote(SocketAgent.class)
public class SocketAgentImpl implements SocketAgent {

	private static final Logger LOG = Logger.getLogger(SocketAgentImpl.class.getName());

	/**
	 * der result pusher wartet immer eine bestimmte zeit auf ein resultat, wenn nichts verfügbar ist, loggt er und
	 * wartet wieder
	 */
	private static final int RESULT_PUSHER_CYCLE_TIMEOUT = 2000;

	/**
	 * Ein SocketAgent hat eine 1:1 Verbindung zum SocketWorker und führt nur einen Task aufs Mal aus
	 */
	private static final int MAX_CONCURRENT_TASKS = 1;

	/** IP - Adresse von diesem Client/Worker */
	private final String clientIp;

	private final TaskRunnerFactory trFactory;

	/** Nimmt tasks vom Master engegeben entgegeben und führt sie aus */
	private final ExecutorService taskRunnerService;

	/** Wartet bis ein Task fertig ist und dessen Resultat zum Master versendet werden kann */
	private final ExecutorService resultPusherService;

	/** Verbindung zum Master um die Resulate zu übergeben */
	private final SocketResultCollector resultCollector;

	/** erstellt resulate aus TaskResults oder Exceptions um zum Master versandt zu werden */
	private final SocketAgentResultFactory sarFactory;

	/**
	 * Nach diesem Timeout wird der Task abgebrochen, tasks können also maximal eine bestimmte Zeit ausgeführt werden.
	 */
	private final long taskRunTimeout;

	/** wie viele tasks der socket agent bisher ausgeführt hat */
	private final AtomicLong runTasks = new AtomicLong();

	/**
	 * Der Task, der gerade ausgeführt werden soll wird in diese Queue gesteckt um dann vom Result-Pusher wieder
	 * herausgenommen zu werden. Double-Ended-Queue: wir füllen vorne rein und nehmen hinten raus.
	 */
	private final BlockingDeque<KeyValuePair<TaskID, Future<TaskResult>>> tasks = new LinkedBlockingDeque<KeyValuePair<TaskID, Future<TaskResult>>>(
			MAX_CONCURRENT_TASKS);

	@Inject
	SocketAgentImpl(@Assisted String clientIp, TaskRunnerFactory trFactory,
			@Named("taskRunnerService") ExecutorService taskRunnerService,
			@Named("resultPusherService") ExecutorService resultPusherService, SocketResultCollector resCollector,
			SocketAgentResultFactory sarFactory, @Named("taskRunTimeout") long taskRunTimeout) {
		this.clientIp = clientIp;
		this.trFactory = trFactory;
		this.taskRunnerService = taskRunnerService;
		this.resultPusherService = resultPusherService;
		this.resultCollector = resCollector;
		this.sarFactory = sarFactory;
		this.taskRunTimeout = taskRunTimeout;
	}

	/** Startet den Service, der immer wieder die Resultate dem Master gibt. */
	@PostConstruct
	public void startResultPusher() {
		LOG.entering(getClass().getName(), "startResultPusher");
		this.resultPusherService.submit(new Runnable() {
			@Override
			public void run() {
				try {
					while (true) {
						LOG.fine("Waiting For Next Task in Queue");
						KeyValuePair<TaskID, Future<TaskResult>> pair = tasks.pollLast(RESULT_PUSHER_CYCLE_TIMEOUT, TimeUnit.MILLISECONDS);
						if (pair == null) {
							LOG.log(Level.INFO, "Waited for {0} ms. Run {1} Tasks so far.", new Object[] {
									RESULT_PUSHER_CYCLE_TIMEOUT, runTasks.get() });
							continue;
						}
						TaskID taskID = pair.getKey();
						Future<TaskResult> task = pair.getValue();
						LOG.log(Level.FINE, "Took Task from Queue. Now waiting for its Completion {0}",
								new Object[] { taskID });

						SocketAgentResult saResult;
						try {
							TaskResult result = task.get(taskRunTimeout, TimeUnit.MILLISECONDS);
							LOG.log(Level.INFO, "Task ran Fine {0}", taskID);
							saResult = sarFactory.createFromTaskResult(taskID.mapReduceTaskUuid, taskID.taskUuid,
									result);
						} catch (TimeoutException e) {
							LOG.info("Task not completed within Timeout: " + taskRunTimeout);
							// timeout abgelaufen, tasks soll nicht weiter ausgeführt werden
							saResult = sarFactory.createFromException(taskID.mapReduceTaskUuid, taskID.taskUuid, e);
							task.cancel(true);
						} catch (Exception e) {
							LOG.log(Level.WARNING, "Task threw Exception", e);
							saResult = sarFactory.createFromException(taskID.mapReduceTaskUuid, taskID.taskUuid, e);
						} finally {
							runTasks.incrementAndGet();
						}

						LOG.finer("Before Pushing");
						resultCollector.pushResult(saResult);
						LOG.finer("After Pushing");
					}
				} catch (InterruptedException interrupted) {
					LOG.info("Pusher Interrupted. Stop Pushing");
				}
			}
		});
		LOG.exiting(getClass().getName(), "startResultPusher");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void helloslave() {
		LOG.info("Successfully registered on Master");
	}

	/**
	 * Diese Methode wird vom Master/SocketWorker aufgerufen, wenn ein Task auf dem Client/Worker ausgeführt weden soll.
	 * Diese Methode soll so wenig wie möglich blockieren und versucht deshalb den Task dem ExecutorService zur
	 * Ausführung zu übergeben und ihn in die Queue einzureihen. In der Queue wird der Task vom Pusher-Service wieder
	 * herausgenommen um das Resultat dem Master zurückzugeben.
	 */
	@Override
	public AgentTaskState runTask(final AgentTask agentTask) {
		String mrUuid = agentTask.getMapReduceTaskUuid();
		String taskUuid = agentTask.getTaskUuid();
		LOG.entering(getClass().getName(), "runTask", new Object[] { mrUuid, taskUuid });

		AgentTaskState state;
		try {
			final TaskRunner runner = trFactory.createTaskRunner(agentTask);
			Future<TaskResult> task = this.taskRunnerService.submit(new Callable<TaskResult>() {
				@Override
				public TaskResult call() throws Exception {
					return runner.runTask();
				};
			});
			TaskID taskID = new TaskID(mrUuid, taskUuid);
			if (this.tasks.offerFirst(new KeyValuePair<TaskID, Future<TaskResult>>(taskID, task))) {
				state = new AgentTaskState(ACCEPTED);
				LOG.info("Accepted Task for Execution");
			} else {
				String msg = "SocketAgent can only run " + MAX_CONCURRENT_TASKS + " Tasks at a time!";
				LOG.warning(msg);
				task.cancel(true); // task wurde schon zur ausführung übergeben, also nehmen wir ihn zurück.
				state = new AgentTaskState(REJECTED, msg);
			}
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Failed to Schedule Task", e);
			state = new AgentTaskState(REJECTED, e.getMessage());
		}
		LOG.log(Level.FINER, "RETURN State={0}, TaskID={1}", new Object[]{state, new TaskID(mrUuid, taskUuid)});
		return state;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getIp() {
		return this.clientIp;
	}

	@Override
	public String toString() {
		return "SocketAgentImpl [ClientIP=" + this.clientIp + "]";
	}

	private static class TaskID {
		final String mapReduceTaskUuid;
		final String taskUuid;

		TaskID(String mapReduceTaskUuid, String taskUuid) {
			this.mapReduceTaskUuid = mapReduceTaskUuid;
			this.taskUuid = taskUuid;
		}

		@Override
		public String toString() {
			return "TaskID [MapReduceTaskUuid=" + this.mapReduceTaskUuid + ",TaskUuid=" + this.taskUuid + "]";
		}
	}

	@Override
	public String ping() {
		LOG.fine("Received Ping from Master");
		return "pong";
	}
}
