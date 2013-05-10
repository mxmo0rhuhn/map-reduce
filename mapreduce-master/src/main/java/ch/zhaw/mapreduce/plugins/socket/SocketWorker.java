package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

	private volatile Future<WorkerTask> currentTask;

	@Inject
	public SocketWorker(@Assisted SocketAgent agent, @Named("taskrunnerservice") ExecutorService exec, Pool pool,
			AgentTaskFactory atFactory, @Assisted SocketResultCollector resCollector) {
		this.agent = agent;
		this.agentIP = agent.getIp();
		this.taskRunnerService = exec;
		this.pool = pool;
		this.atFactory = atFactory;
		this.resCollector = resCollector;
	}

	@Override
	public void resultAvailable(String mapReduceTaskUuid, String taskUuid, boolean success) {
		LOG.entering(getClass().getName(), "resultAvailable", new Object[] { mapReduceTaskUuid, taskUuid, success });
		Future<WorkerTask> future = this.currentTask;
		if (future == null) {
			LOG.log(Level.SEVERE, "Got notified for missing Task {0} {1}", new Object[] { mapReduceTaskUuid, taskUuid });
			return;
		}
		try {
			// sollte nie blockieren, aber wir geben ihm ein bisschen zeit
			WorkerTask task = future.get(100, TimeUnit.MILLISECONDS);
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
		} catch (Exception e) {
			LOG.log(Level.SEVERE, "Failed to retrieve WorkerTask from Future {0} {1}", new Object[] {
					mapReduceTaskUuid, taskUuid });
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
		if (this.currentTask != null) {
			LOG.severe("SocketWorker is already Running a Task!");
			workerTask.failed();
			return;
		}
		this.currentTask = this.taskRunnerService.submit(new Callable<WorkerTask>() {

			@Override
			public WorkerTask call() throws Exception {
				AgentTask agentTask = atFactory.createAgentTask(workerTask);
				AgentTaskState state = agent.runTask(agentTask); // RPC
				switch (state.state()) {
				case ACCEPTED:
					LOG.fine("Task Accepted by SocketAgent");
					workerTask.started();
					resCollector.registerObserver(workerTask.getMapReduceTaskUuid(), workerTask.getTaskUuid(),
							SocketWorker.this);
					break;
				case REJECTED:
					LOG.warning("Task Rejected by SocketAgent: " + state.msg());
					workerTask.failed();
					pool.workerIsFinished(SocketWorker.this);
					break;
				default:
					throw new IllegalStateException("Unhandle: " + state.state());
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
		// TODO FIXME
		throw new UnsupportedOperationException("Missing feature for SocketAgents. Implement Me!");
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+" for SocketAgent with IP="+this.agentIP;
	}

}
