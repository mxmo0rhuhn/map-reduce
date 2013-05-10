package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.api.ThreadingPolicy;
import org.jmock.auto.Auto;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Test;

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
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.ACCEPTED)));
			oneOf(workerTask).started();
			oneOf(workerTask).getMapReduceTaskUuid(); will(returnValue(mrUuid));
			oneOf(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(resCollector).registerObserver(mrUuid, taskUuid, sw);
			then(taskRunnerState.is("runningTask"));
		}});
		sw.executeTask(workerTask);
		sync.waitUntil(taskRunnerState.is("runningTask"), 200);
	}
	
	@Test
	public void shouldGoBackToPoolIfTaskIsRejected() throws Exception {
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector);
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.REJECTED)));
			oneOf(workerTask).failed();
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
		ExecutorService taskRunnerSrv = Executors.newSingleThreadExecutor();
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		final SocketWorker sw = new SocketWorker(sAgent, taskRunnerSrv, p, atFactory, resCollector);
		p.donateWorker(sw);
		assertEquals(1, p.getFreeWorkers());
		taskRunnerState.startsAs("beforeRunning");
		mockery.checking(new Expectations() {{ 
			oneOf(workerTask).enqueued();
			oneOf(workerTask).setWorker(sw);
			oneOf(atFactory).createAgentTask(workerTask); will(returnValue(agentTask));
			oneOf(sAgent).runTask(agentTask); will(returnValue(new AgentTaskState(State.ACCEPTED)));
			oneOf(workerTask).started();
			atLeast(1).of(workerTask).getMapReduceTaskUuid(); will(returnValue(mrUuid));
			atLeast(1).of(workerTask).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(resCollector).registerObserver(mrUuid, taskUuid, sw);
			then(taskRunnerState.is("taskRunning"));
			oneOf(workerTask).completed();
			then(taskRunnerState.is("taskDone"));
		}});
		p.enqueueTask(workerTask);
		sync.waitUntil(taskRunnerState.is("taskRunning"), 200);
		sw.resultAvailable(mrUuid, taskUuid, true);
		sync.waitUntil(taskRunnerState.is("taskDone"), 200);
		Thread.yield();
		Thread.sleep(200); // geht erst nach dem task.failed aufruf in den pool, also noch kurz warten
		assertEquals(1, p.getFreeWorkers());
	}

}
