package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

public class SocketWorkerTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery() {
		{
			// macht mocken von klassen moeglich. bizeli grusig, aber passt
			setImposteriser(ClassImposteriser.INSTANCE);
			setThreadingPolicy(new Synchroniser());
		}
	};

	@Mock
	private Persistence persistence;

	@Mock
	private SocketAgent agent;

	@Mock
	private MapWorkerTask mapTask;

	@Mock
	private ReduceWorkerTask reduceTask;

	private final String mrUuid = "mapreduceuuid";

	private final String taskUuid = "taskuuid";

	private final List<KeyValuePair> mapResult = Arrays.asList(new KeyValuePair[] { new KeyValuePair("key1", "val1") });

	private final List<String> reduceResult = Arrays.asList(new String[] { "res1" });

	@Test
	public void shouldRunMapTaskOnCallback() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		SocketWorker worker = new SocketWorker(agent, exec, persistence, p);
		this.mockery.checking(new Expectations() {
			{
				atLeast(1).of(mapTask).getMapReduceTaskUUID();
				will(returnValue(mrUuid));
				atLeast(1).of(mapTask).getUUID();
				will(returnValue(taskUuid));
				oneOf(agent).runTask(mapTask);
				will(returnValue(mapResult));
				oneOf(persistence).storeMap(mrUuid, taskUuid, "key1", "val1");
			}
		});
		worker.executeTask(mapTask);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
	}

	@Test
	public void shouldRunReduceTaskOnCallback() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		SocketWorker worker = new SocketWorker(agent, exec, persistence, p);
		this.mockery.checking(new Expectations() {
			{
				atLeast(1).of(reduceTask).getMapReduceTaskUUID();
				will(returnValue(mrUuid));
				atLeast(1).of(reduceTask).getUUID();
				will(returnValue(taskUuid));
				oneOf(agent).runTask(reduceTask);
				will(returnValue(reduceResult));
				oneOf(persistence).storeReduce(mrUuid, taskUuid, "res1");
			}
		});
		worker.executeTask(reduceTask);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
	}

	@Test
	public void shouldGoBackToPool() throws Exception {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		final SocketWorker worker = new SocketWorker(agent, exec, persistence, p);
		p.donateWorker(worker);
		assertEquals(1, p.getFreeWorkers());
		this.mockery.checking(new Expectations() {
			{
				oneOf(mapTask).setWorker(worker);
				atLeast(1).of(mapTask).getMapReduceTaskUUID();
				will(returnValue(mrUuid));
				atLeast(1).of(mapTask).getUUID();
				will(returnValue(taskUuid));
				oneOf(agent).runTask(mapTask);
				will(returnValue(mapResult));
				oneOf(persistence).storeMap(mrUuid, taskUuid, "key1", "val1");
			}
		});
		p.enqueueWork(mapTask);
		Thread.yield();
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertEquals(1, p.getFreeWorkers());
	}
	
	@Test
	public void shouldGoBackOnFailure() throws Exception {
		fail("implement me");
	}

}
