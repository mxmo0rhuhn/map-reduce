package ch.zhaw.mapreduce.impl;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

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

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

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
			@Override public void doWork(Worker processingWorker) { }
            @Override public State getCurrentState() { return null; }
			@Override public Worker getWorker() { return null; }
			@Override public String getUUID() { return null; }
			@Override public String getMapReduceTaskUUID() { return mrtuuid; } 
		};
		this.workerTask2 = new WorkerTask() {
			@Override public void doWork(Worker processingWorker) { }
            @Override public State getCurrentState() { return null; }
			@Override public Worker getWorker() { return null; }
			@Override public String getUUID() { return null; }
			@Override public String getMapReduceTaskUUID() { return mrtuuid2; } 
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
		worker.execute(task);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
	}

	@Test
	public void shouldReturnPreviouslyStoredMapValues() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.execute(this.workerTask);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key", "value"));
		List<KeyValuePair> vals = worker.getMapResults(mrtuuid);
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.size() == 1);
	}

	@Test
	public void shouldReturnPreviouslyStoredReduceValues() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.execute(this.workerTask);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		List<KeyValuePair> vals = worker.getReduceResults(mrtuuid);
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.size() == 1);
	}

	@Test
	public void shouldAssociateResultsForSameMapTask() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.execute(this.workerTask);
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
		worker.execute(this.workerTask);
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
		worker.execute(workerTask);
		worker.execute(workerTask2);
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
		worker.execute(workerTask);
		worker.execute(workerTask2);
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
		worker.execute(workerTask);
		worker.storeReduceResult(mrtuuid, new KeyValuePair("key", "value"));
		assertEquals(0, worker.getMapResults(mrtuuid).size());
	}

	@Test
	public void shouldNotMixMapResults() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.execute(workerTask);
		worker.storeMapResult(mrtuuid, new KeyValuePair("key", "value"));
		assertEquals(0, worker.getReduceResults(mrtuuid).size());
	}


}
