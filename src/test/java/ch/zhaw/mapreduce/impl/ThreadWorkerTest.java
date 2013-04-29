package ch.zhaw.mapreduce.impl;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.DeterministicExecutor;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.plugins.thread.ThreadWorker;

@RunWith(JMock.class)
public class ThreadWorkerTest {

	private Mockery context;
	
	private WorkerTask task;

	private WorkerTask workerTask1;
	
	private WorkerTask workerTask2;
	
	private final String mrtuuid = "mrtuuid";
	
	private final String mrtuuid2 = "mrtuuid2";
	
	private ContextFactory ctxFactory;
	
	private Context ctx;

	@Before
	public void initMockery() {
		this.context = new JUnit4Mockery();
		this.ctxFactory = this.context.mock(ContextFactory.class);
		this.task = this.context.mock(WorkerTask.class);
		this.ctx = this.context.mock(Context.class);
		this.workerTask1 = new WorkerTask() {
			@Override public void runTask(Context ctx) { }
            @Override public State getCurrentState() { return null; }
			@Override public Worker getWorker() { return null; }
			@Override public String getUUID() { return null; }
			@Override public String getMapReduceTaskUUID() { return mrtuuid; }
			@Override public void setWorker(Worker worker) { }
			@Override public String getInput() { return null; }
			@Override public void setState(State newState) { } 
		};
		this.workerTask2 = new WorkerTask() {
			@Override public void runTask(Context ctx) { }
            @Override public State getCurrentState() { return null; }
			@Override public Worker getWorker() { return null; }
			@Override public String getUUID() { return null; }
			@Override public String getMapReduceTaskUUID() { return mrtuuid2; } 
			@Override public void setWorker(Worker worker) { }
			@Override public String getInput() { return null; } 
			@Override public void setState(State newState) { } 
		};
	}

	@Test
	public void shouldGoBackToPool() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		final ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		pool.donateWorker(worker);
		this.context.checking(new Expectations() {{
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			}});
		worker.executeTask(task);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
		assertEquals(1, pool.getFreeWorkers());
	}
	
	@Test
	public void shouldReturnEmpyListForUnknownMapReduceIdForMapResult() {
		fail("implement me");
	}
	
	@Test
	public void shouldReturnEmpyListForKnownMapReduceIdButUnknownTaskUuidForMapResult() {
		fail("implement me");
	}
	
	@Test
	public void shouldReturnEmpyListForUnknownMapReduceIdForReduceResult() {
		fail("implement me");
	}

	@Test
	public void shouldReturnEmpyListForKnownMapReduceIdButUnknownTaskUuidForReduceResult() {
		fail("implement me");
	}
	
	@Test
	public void shouldReturnMapValuesFromContext() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("hello", "1")});
		this.context.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			
			// returning values from context
			oneOf(ctx).getMapResult(); will(returnValue(result));
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result, worker.getMapResult("mrtUuid", "taskUuid"));
	}

	@Test
	public void shouldReturnPreviouslyStoredReduceValues() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(this.workerTask1);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		List<KeyValuePair> vals = worker.getReduceResults(mrtuuid);
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.size() == 1);
	}

	@Test
	public void shouldAssociateResultsForSameMapTask() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(this.workerTask1);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key", "value"));
		worker.storeMapResult(mrtuuid, new KeyValuePair("key2", "value"));
		List<KeyValuePair> vals = worker.getMapResults(mrtuuid);
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.contains(new KeyValuePair("key2", "value")));
		assertTrue(vals.size() == 2);
	}

	@Test
	public void shouldAssociateResultsForSameReduceTask() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(this.workerTask1);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key2", "value"));
		List<KeyValuePair> vals = worker.getReduceResults(mrtuuid);
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.contains(new KeyValuePair("key2", "value")));
		assertTrue(vals.size() == 2);
	}

	@Test
	public void shouldHandleMultipleMapUIds() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask1);
		worker.executeTask(workerTask2);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key", "value"));
		worker.storeMapResult(mrtuuid2, new KeyValuePair("key2", "value2"));
		List<KeyValuePair> vals1 = worker.getMapResults(mrtuuid);
		List<KeyValuePair> vals2 = worker.getMapResults(mrtuuid2);
		assertTrue(vals1.contains(new KeyValuePair("key", "value")));
		assertTrue(vals2.contains(new KeyValuePair("key2", "value2")));
		assertTrue(vals1.size() == 1);
		assertTrue(vals2.size() == 1);
	}

	@Test
	public void shouldHandleMultipleReduceUIds() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask1);
		worker.executeTask(workerTask2);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		worker.storeReduceResult(mrtuuid2, new KeyValuePair("key2", "value2"));
		List<KeyValuePair> vals1 = worker.getReduceResults(mrtuuid);
		List<KeyValuePair> vals2 = worker.getReduceResults(mrtuuid2);
		assertTrue(vals1.contains(new KeyValuePair("key", "value")));
		assertTrue(vals2.contains(new KeyValuePair("key2", "value2")));
		assertTrue(vals1.size() == 1);
		assertTrue(vals2.size() == 1);
	}

	@Test
	public void shouldNotMixReduceResults() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask1);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		assertEquals(0, worker.getMapResults(mrtuuid).size());
	}

	@Test
	public void shouldNotMixMapResults() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask1);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key", "value"));
		assertEquals(0, worker.getReduceResults(mrtuuid).size());
	}
	
	@Test
	public void shouldNotOverrideExistingResultsForSameMapreducetaskuuid() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask1);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key1", "value1"));
		worker.executeTask(workerTask1);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key2", "value2"));
		assertTrue(worker.getMapResults(mrtuuid).contains(new KeyValuePair("key1", "value1")));
	}
	
	@Test(expected=IllegalStateException.class)
	public void shouldNotAcceptMapResultIfNotInitialized() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key1", "value1"));
	}

	@Test(expected=IllegalStateException.class)
	public void shouldNotAcceptReduceResultIfNotInitialized() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key1", "value1"));
	}
	
	@Test
	public void shoudReplaceResult() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask1);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key1", "value1"));
		assertEquals(1, worker.getMapResults(mrtuuid).size());
		assertEquals(new KeyValuePair("key1", "value1"), worker.getMapResults(mrtuuid).get(0));
		
		worker.replaceMapResult(mrtuuid, Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "value2")}));
		assertEquals(1, worker.getMapResults(mrtuuid).size());
		assertEquals(new KeyValuePair("key1", "value2"), worker.getMapResults(mrtuuid).get(0));
	}

	@Test
	public void shoudNotTouchOtherWhenReplacing() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask1);
		worker.executeTask(workerTask2);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key1", "value1"));
		worker.storeMapResult(mrtuuid2, new KeyValuePair("key1", "value1"));
		
		worker.replaceMapResult(mrtuuid, Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "value2")}));
		assertEquals(1, worker.getMapResults(mrtuuid2).size());
		assertEquals(new KeyValuePair("key1", "value1"), worker.getMapResults(mrtuuid2).get(0));
	}

}
