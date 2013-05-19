package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.api.ThreadingPolicy;
import org.jmock.auto.Auto;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.plugins.socket.AgentTaskState.State;

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
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService, 2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.ACCEPTED)));
			oneOf(workerTask).started();
			oneOf(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(resCollector).registerObserver(taskUuid, sw); will(returnValue(null));
			then(taskRunnerState.is("runningTask"));
		}});
		sw.executeTask(workerTask);
		sync.waitUntil(taskRunnerState.is("runningTask"), 200);
	}
	
	@Test
	public void shouldSetToCompleteImmediately() throws Exception {
		allowGetIp();
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor(), 1, 2, schedService, 1);
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService, 2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.ACCEPTED)));
			oneOf(workerTask).started();
			oneOf(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(resCollector).registerObserver(taskUuid, sw); will(returnValue(saRes));
			oneOf(saRes).wasSuccessful(); will(returnValue(true));
			oneOf(saRes).getResult(); will(returnValue(new ArrayList<String>()));
			oneOf(workerTask).successful(with(aNonNull(List.class)));
			then(taskRunnerState.is("runningTask"));
		}});
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
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService, 2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.ACCEPTED)));
			oneOf(workerTask).started();
			oneOf(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(resCollector).registerObserver(taskUuid, sw); will(returnValue(saRes));
			oneOf(saRes).wasSuccessful(); will(returnValue(false));
			oneOf(workerTask).fail();
			oneOf(saRes).getException(); will(returnValue(new Exception()));
			then(taskRunnerState.is("runningTask"));
		}});
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
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService, 2000);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.REJECTED)));
			oneOf(workerTask).fail();
			then(taskRunnerState.is("runningTask"));
		}});
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
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService, 2000);
		p.donateWorker(sw);
		assertEquals(1, p.getFreeWorkers());
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(workerTask).enqueued();
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.ACCEPTED)));
			oneOf(workerTask).started();
			atLeast(1).of(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(resCollector).registerObserver(taskUuid, sw); will(returnValue(null));
			then(taskRunnerState.is("taskRunning"));
			oneOf(saRes).getResult(); will(returnValue(new ArrayList<KeyValuePair>()));
			oneOf(saRes).wasSuccessful(); will(returnValue(true));
			oneOf(workerTask).successful(with(aNonNull(List.class)));
			then(taskRunnerState.is("taskDone"));
		}});
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
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector, 200, schedService, 2000);
		p.donateWorker(sw);
		assertEquals(1, p.getFreeWorkers());
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(workerTask).enqueued();
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.ACCEPTED)));
			oneOf(workerTask).started();
			atLeast(1).of(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(resCollector).registerObserver(taskUuid, sw); will(returnValue(null));
			then(taskRunnerState.is("taskRunning"));
			oneOf(saRes).wasSuccessful(); will(returnValue(false));
			oneOf(saRes).getException(); will(returnValue(new Exception()));
			oneOf(workerTask).fail();
			then(taskRunnerState.is("taskDone"));
		}});
		p.enqueueTask(workerTask);
		sync.waitUntil(taskRunnerState.is("taskRunning"), 200);
		sw.resultAvailable(taskUuid, saRes);
		sync.waitUntil(taskRunnerState.is("taskDone"), 200);
		Thread.yield();
		Thread.sleep(200); // geht erst nach dem task.failed aufruf in den pool, also noch kurz warten
		assertEquals(1, p.getFreeWorkers());
	}
	
	void allowGetIp() {
		mockery.checking(new Expectations() {{ 
			allowing(sAgent).getIp();
		}});
	}

}
