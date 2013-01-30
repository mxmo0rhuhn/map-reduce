package ch.zhaw.mapreduce.impl;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
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
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.workers.ThreadWorker;
import ch.zhaw.mapreduce.workers.Worker;

@RunWith(JMock.class)
public class ThreadWorkerTest {

	private Mockery context;

	private Pool pool;
	
	private WorkerTask workerTask;
	
	private WorkerTask workerTask2;
	
	private final String mrtuuid = "mrtuuid";
	
	private final String mrtuuid2 = "mrtuuid2";

	@Before
	public void initMockery() {
		this.context = new JUnit4Mockery();
		this.pool = this.context.mock(Pool.class);
		this.workerTask = new WorkerTask() {
			@Override public void runTask(Context ctx) { }
            @Override public State getCurrentState() { return null; }
			@Override public Worker getWorker() { return null; }
			@Override public String getUUID() { return null; }
			@Override public String getMapReduceTaskUUID() { return mrtuuid; }
			@Override public void setWorker(Worker worker) { }
			@Override public List<KeyValuePair> getResults(String mapReduceTaskUUID) { return null; }
			@Override public String getInput() { return null; } 
		};
		this.workerTask2 = new WorkerTask() {
			@Override public void runTask(Context ctx) { }
            @Override public State getCurrentState() { return null; }
			@Override public Worker getWorker() { return null; }
			@Override public String getUUID() { return null; }
			@Override public String getMapReduceTaskUUID() { return mrtuuid2; } 
			@Override public void setWorker(Worker worker) { }
			@Override public List<KeyValuePair> getResults(String mapReduceTaskUUID) { return null; }
			@Override public String getInput() { return null; } 
		};
	}

	@Test
	public void shouldGoBackToPool() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		final ThreadWorker worker = new ThreadWorker(pool, exec);
		final WorkerTask task = this.context.mock(WorkerTask.class);
		final Sequence seq = this.context.sequence("executionOrder");
		this.context.checking(new Expectations() {
			{
				exactly(2).of(task).getMapReduceTaskUUID();
				inSequence(seq);
				oneOf(task).doWork(worker);
				inSequence(seq);
				oneOf(pool).workerIsFinished(worker);
			}
		});
		worker.executeTask(task);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
	}

	@Test
	public void shouldReturnPreviouslyStoredMapValues() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(this.workerTask);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key", "value"));
		List<KeyValuePair> vals = worker.getMapResults(mrtuuid);
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.size() == 1);
	}

	@Test
	public void shouldReturnPreviouslyStoredReduceValues() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(this.workerTask);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		List<KeyValuePair> vals = worker.getReduceResults(mrtuuid);
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.size() == 1);
	}

	@Test
	public void shouldAssociateResultsForSameMapTask() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(this.workerTask);
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
		worker.executeTask(this.workerTask);
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
		worker.executeTask(workerTask);
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
		worker.executeTask(workerTask);
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
		worker.executeTask(workerTask);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		assertEquals(0, worker.getMapResults(mrtuuid).size());
	}

	@Test
	public void shouldNotMixMapResults() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key", "value"));
		assertEquals(0, worker.getReduceResults(mrtuuid).size());
	}
	
	@Test
	public void shouldNotOverrideExistingResultsForSameMapreducetaskuuid() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.executeTask(workerTask);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key1", "value1"));
		worker.executeTask(workerTask);
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
		worker.executeTask(workerTask);
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
		worker.executeTask(workerTask);
		worker.executeTask(workerTask2);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key1", "value1"));
		worker.storeMapResult(mrtuuid2, new KeyValuePair("key1", "value1"));
		
		worker.replaceMapResult(mrtuuid, Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "value2")}));
		assertEquals(1, worker.getMapResults(mrtuuid2).size());
		assertEquals(new KeyValuePair("key1", "value1"), worker.getMapResults(mrtuuid2).get(0));
	}

}
