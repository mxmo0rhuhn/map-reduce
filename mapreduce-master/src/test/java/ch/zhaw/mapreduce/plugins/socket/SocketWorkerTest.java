package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.api.ThreadingPolicy;
import org.jmock.auto.Auto;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Test;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.plugins.socket.AgentTaskState.State;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketResultCollectorImpl;

public class SocketWorkerTest extends AbstractMapReduceMasterSocketTest {

	private Synchroniser sync;

	@Auto
	private States taskRunnerState;

	@Override
	protected ThreadingPolicy useThreadingPolicy() {
		return (this.sync = new Synchroniser());
	}

	@Test
	public void shoudlRunTask() throws Exception {
		allowGetIp();
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor(), 1, 2, schedService, 1);
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService,
				2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {
			{
				oneOf(atFactory).createAgentTask(workerTask);
				will(returnValue(agentTask));
				oneOf(sAgent).runTask(agentTask);
				will(returnValue(new AgentTaskState(State.ACCEPTED)));
				oneOf(workerTask).started();
				oneOf(workerTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(resCollector).registerObserver(taskUuid, sw);
				will(returnValue(null));
				then(taskRunnerState.is("runningTask"));
			}
		});
		sw.executeTask(workerTask);
		sync.waitUntil(taskRunnerState.is("runningTask"), 200);
	}

	@Test
	public void shouldSetToCompleteImmediately() throws Exception {
		allowGetIp();
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor(), 1, 2, schedService, 1);
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService,
				2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {
			{
				oneOf(atFactory).createAgentTask(workerTask);
				will(returnValue(agentTask));
				oneOf(sAgent).runTask(agentTask);
				will(returnValue(new AgentTaskState(State.ACCEPTED)));
				oneOf(workerTask).started();
				oneOf(workerTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(resCollector).registerObserver(taskUuid, sw);
				will(returnValue(saRes));
				oneOf(saRes).wasSuccessful();
				will(returnValue(true));
				oneOf(saRes).getResult();
				will(returnValue(new ArrayList<String>()));
				oneOf(workerTask).successful(with(aNonNull(List.class)));
				then(taskRunnerState.is("runningTask"));
			}
		});
		sw.executeTask(workerTask);
		sync.waitUntil(taskRunnerState.is("runningTask"), 200);
		Thread.yield();
		Thread.sleep(200);
		assertEquals(1, p.getFreeWorkers());
	}

	@Test
	public void shouldSetToFailedImmediately() throws Exception {
		allowGetIp();
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor(), 1, 2, schedService, 1);
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService,
				2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {
			{
				oneOf(atFactory).createAgentTask(workerTask);
				will(returnValue(agentTask));
				oneOf(sAgent).runTask(agentTask);
				will(returnValue(new AgentTaskState(State.ACCEPTED)));
				oneOf(workerTask).started();
				oneOf(workerTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(resCollector).registerObserver(taskUuid, sw);
				will(returnValue(saRes));
				oneOf(saRes).wasSuccessful();
				will(returnValue(false));
				oneOf(workerTask).fail();
				oneOf(saRes).getException();
				will(returnValue(new Exception()));
				then(taskRunnerState.is("runningTask"));
			}
		});
		sw.executeTask(workerTask);
		sync.waitUntil(taskRunnerState.is("runningTask"), 200);
		Thread.yield();
		Thread.sleep(200);
		assertEquals(1, p.getFreeWorkers());
	}

	@Test
	public void shouldGoBackToPoolIfTaskIsRejected() throws Exception {
		allowGetIp();
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor(), 1, 2, schedService, 1);
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService,
				2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {
			{
				oneOf(workerTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(atFactory).createAgentTask(workerTask);
				will(returnValue(agentTask));
				oneOf(sAgent).runTask(agentTask);
				will(returnValue(new AgentTaskState(State.REJECTED)));
				oneOf(workerTask).fail();
				then(taskRunnerState.is("runningTask"));
			}
		});
		sw.executeTask(workerTask);
		sync.waitUntil(taskRunnerState.is("runningTask"), 200);
		Thread.yield();
		Thread.sleep(200); // geht erst nach dem task.failed aufruf in den pool, also noch kurz warten
		assertEquals(1, p.getFreeWorkers());
	}

	@Test
	public void shouldGoBackToPoolWhenTaskIsFinished() throws Exception {
		allowGetIp();
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor(), 1, 2, schedService, 1);
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService,
				2000);
		p.donateWorker(sw);
		assertEquals(1, p.getFreeWorkers());
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {
			{
				oneOf(workerTask).enqueued();
				oneOf(atFactory).createAgentTask(workerTask);
				will(returnValue(agentTask));
				oneOf(sAgent).runTask(agentTask);
				will(returnValue(new AgentTaskState(State.ACCEPTED)));
				oneOf(workerTask).started();
				atLeast(1).of(workerTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(resCollector).registerObserver(taskUuid, sw);
				will(returnValue(null));
				then(taskRunnerState.is("taskRunning"));
				oneOf(saRes).getResult();
				will(returnValue(new ArrayList<KeyValuePair>()));
				oneOf(saRes).wasSuccessful();
				will(returnValue(true));
				oneOf(workerTask).successful(with(aNonNull(List.class)));
				then(taskRunnerState.is("taskDone"));
			}
		});
		p.enqueueTask(workerTask);
		sync.waitUntil(taskRunnerState.is("taskRunning"), 200);
		sw.resultAvailable(taskUuid, saRes);
		sync.waitUntil(taskRunnerState.is("taskDone"), 200);
		Thread.yield();
		Thread.sleep(200); // geht erst nach dem task.failed aufruf in den pool, also noch kurz warten
		assertEquals(1, p.getFreeWorkers());
	}

	@Test
	public void shouldGoBackToPoolWhenTaskHasFailed() throws Exception {
		allowGetIp();
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor(), 1, 2, schedService, 1);
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService,
				2000);
		p.donateWorker(sw);
		assertEquals(1, p.getFreeWorkers());
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {
			{
				oneOf(workerTask).enqueued();
				oneOf(atFactory).createAgentTask(workerTask);
				will(returnValue(agentTask));
				oneOf(sAgent).runTask(agentTask);
				will(returnValue(new AgentTaskState(State.ACCEPTED)));
				oneOf(workerTask).started();
				atLeast(1).of(workerTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(resCollector).registerObserver(taskUuid, sw);
				will(returnValue(null));
				then(taskRunnerState.is("taskRunning"));
				oneOf(saRes).wasSuccessful();
				will(returnValue(false));
				oneOf(saRes).getException();
				will(returnValue(new Exception()));
				oneOf(workerTask).fail();
				then(taskRunnerState.is("taskDone"));
			}
		});
		p.enqueueTask(workerTask);
		sync.waitUntil(taskRunnerState.is("taskRunning"), 200);
		sw.resultAvailable(taskUuid, saRes);
		sync.waitUntil(taskRunnerState.is("taskDone"), 200);
		Thread.yield();
		Thread.sleep(200); // geht erst nach dem task.failed aufruf in den pool, also noch kurz warten
		assertEquals(1, p.getFreeWorkers());
	}

	void allowGetIp() {
		mockery.checking(new Expectations() {
			{
				allowing(sAgent).getIp();
			}
		});
	}

	@Test
	public void concurrencyTest() throws InterruptedException {
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		SocketResultCollector resColl = new SocketResultCollectorImpl();
		AgentTaskFactory atFactory = new TestAgentTaskFactory();
		Pool pool = new Pool(Executors.newSingleThreadExecutor(), 1000, 0, Executors.newScheduledThreadPool(1), 10000);
		pool.init();
		long pingertimeout = 300;
		long triggertimeout = 200;
		int nworker = 20;
		int ntasks = 100000;

		ExecutorService workerExec = Executors.newFixedThreadPool(nworker);
		ExecutorService agentExec = Executors.newFixedThreadPool(nworker);
		List<TestSocketAgent> agents = new ArrayList<TestSocketAgent>(nworker);
		for (int i = 0; i < nworker; i++) {
			TestSocketAgent agent = new TestSocketAgent(20, agentExec, resColl);
			agents.add(agent);
			SocketWorker sw = new SocketWorker(agent, workerExec, pool, atFactory, resColl, triggertimeout, scheduler, pingertimeout);
			sw.startAgentPinger();
			pool.donateWorker(sw);
		}
		for (int i = 0; i < ntasks; i++) {
			pool.enqueueTask(new TestTask());
		}

		int enqueued;
		while ((enqueued = pool.enqueuedTasks()) != 0 || pool.getFreeWorkers() != pool.getCurrentPoolSize()) {
			Thread.sleep(1000);
			System.out.println("waiting: " + enqueued);
		}

		workerExec.shutdown();
		agentExec.shutdown();
		workerExec.awaitTermination(10, TimeUnit.SECONDS);
		agentExec.awaitTermination(10, TimeUnit.SECONDS);
		assertTrue(workerExec.shutdownNow().isEmpty());
		assertTrue(agentExec.shutdownNow().isEmpty());

		assertEquals(nworker, pool.getFreeWorkers());
		assertEquals(0, resColl.getResultStates().size() - countRejections(agents));
	}

	static int countRejections(List<TestSocketAgent> agents) {
		int sum = 0;
		for (TestSocketAgent agent : agents) {
			sum += agent.nrejected;
		}
		return sum;
	}

}

class TestTask implements WorkerTask {

	private State state = State.INITIATED;

	private static AtomicLong taskIdCounter = new AtomicLong();

	private final String taskUuid = "task-" + taskIdCounter.incrementAndGet();

	@Override
	public void runTask(Context ctx) {
	}

	@Override
	public State getCurrentState() {
		return state;
	}

	@Override
	public String getTaskUuid() {
		return taskUuid;
	}

	@Override
	public String getInput() {
		return "";
	}

	@Override
	public void abort() {
		state = State.ABORTED;
	}

	@Override
	public Persistence getPersistence() {
		return null;
	}

	@Override
	public void enqueued() {
		state = State.ENQUEUED;
	}

	@Override
	public void fail() {
		state = State.FAILED;
	}

	@Override
	public void started() {
		state = State.INPROGRESS;
	}

	@Override
	public void successful(List<?> result) {
	}

	@Override
	public String toString() {
		return "WorkerTask " + taskUuid;
	}

}

class TestSocketAgent implements SocketAgent {

	private static Random rand = new Random();

	private final int failfrequency;

	private final ExecutorService svc;

	private final SocketResultCollector resColl;

	private int ntasks;

	int nrejected;

	TestSocketAgent(int failfrequency, ExecutorService exec, SocketResultCollector resColl) {
		this.failfrequency = failfrequency;
		this.svc = exec;
		this.resColl = resColl;
	}

	@Override
	public void helloslave() {
	}

	@Override
	public AgentTaskState runTask(final AgentTask task) {
		svc.submit(new Callable<Void>() {
			@SuppressWarnings("serial")
			@Override
			public Void call() throws Exception {
				System.out.println("Running " + task.getTaskUuid());
				Thread.yield();
				Thread.sleep(30);
				// String a = "";
				// for (int i = 0; i < rand.nextInt(1000); i++) {
				// a += i;
				// }
				resColl.pushResult(new SocketAgentResult() {
					@Override
					public boolean wasSuccessful() {
						return true;
						//return rand.nextBoolean() || rand.nextBoolean() || rand.nextBoolean();
					}

					@Override
					public String getTaskUuid() {
						return task.getTaskUuid();
					}

					@Override
					public List<?> getResult() {
						return Collections.emptyList();
					}

					@Override
					public Exception getException() {
						return new Exception();
					}
				});
				return null;
			}
		});
		if (++ntasks % failfrequency == 0) {
			nrejected++;
			return new AgentTaskState(State.REJECTED);
		} else {
			return new AgentTaskState(State.ACCEPTED);
		}
	}

	@Override
	public String getIp() {
		return "NOIP";
	}

	@Override
	public String ping() {
		if (rand.nextInt(1000) > 990) {
			throw new RuntimeException("idie");
		} else {
			return "pong";
		}
	}

}

class TestAgentTaskFactory implements AgentTaskFactory {

	@SuppressWarnings("serial")
	@Override
	public AgentTask createAgentTask(final WorkerTask workerTask) {
		return new AgentTask() {

			@Override
			public String getTaskUuid() {
				return workerTask.getTaskUuid();
			}
		};
	}

}