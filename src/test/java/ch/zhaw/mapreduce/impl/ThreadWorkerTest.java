package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

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
import ch.zhaw.mapreduce.WorkerTask;

@RunWith(JMock.class)
public class ThreadWorkerTest {

	private Mockery context;

	private Pool pool;

	@Before
	public void initMockery() {
		this.context = new JUnit4Mockery();
		this.pool = this.context.mock(Pool.class);
	}

	@Test
	public void shouldGoBackToPool() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		final ThreadWorker worker = new ThreadWorker(pool, exec);
		final WorkerTask task = this.context.mock(WorkerTask.class);
		final Sequence seq = this.context.sequence("executionOrder");
		worker.execute(task);
		this.context.checking(new Expectations() {
			{
				oneOf(task).doWork(worker);
				inSequence(seq);
				oneOf(pool).workerIsFinished(worker);
			}
		});
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
	}

	@Test
	public void shouldReturnPreviouslyStoredMapValues() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeMapResult("mrtuid", new KeyValuePair("key", "value"));
		List<KeyValuePair> vals = worker.getMapResults("mrtuid");
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.size() == 1);
	}

	@Test
	public void shouldReturnPreviouslyStoredReduceValues() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeReduceResult("mrtuid", new KeyValuePair("key", "value"));
		List<KeyValuePair> vals = worker.getReduceResults("mrtuid");
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.size() == 1);
	}

	@Test
	public void shouldAssociateResultsForSameMapTask() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeMapResult("mrtuid", new KeyValuePair("key", "value"));
		worker.storeMapResult("mrtuid", new KeyValuePair("key2", "value"));
		List<KeyValuePair> vals = worker.getMapResults("mrtuid");
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.contains(new KeyValuePair("key2", "value")));
		assertTrue(vals.size() == 2);
	}

	@Test
	public void shouldAssociateResultsForSameReduceTask() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeReduceResult("mrtuid", new KeyValuePair("key", "value"));
		worker.storeReduceResult("mrtuid", new KeyValuePair("key2", "value"));
		List<KeyValuePair> vals = worker.getReduceResults("mrtuid");
		assertTrue(vals.contains(new KeyValuePair("key", "value")));
		assertTrue(vals.contains(new KeyValuePair("key2", "value")));
		assertTrue(vals.size() == 2);
	}

	@Test
	public void shouldHandleMultipleMapUIds() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeMapResult("mrtuid1", new KeyValuePair("key", "value"));
		worker.storeMapResult("mrtuid2", new KeyValuePair("key2", "value2"));
		List<KeyValuePair> vals1 = worker.getMapResults("mrtuid1");
		List<KeyValuePair> vals2 = worker.getMapResults("mrtuid2");
		assertTrue(vals1.contains(new KeyValuePair("key", "value")));
		assertTrue(vals2.contains(new KeyValuePair("key2", "value2")));
		assertTrue(vals1.size() == 1);
		assertTrue(vals2.size() == 1);
	}

	@Test
	public void shouldHandleMultipleReduceUIds() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeReduceResult("mrtuid1", new KeyValuePair("key", "value"));
		worker.storeReduceResult("mrtuid2", new KeyValuePair("key2", "value2"));
		List<KeyValuePair> vals1 = worker.getReduceResults("mrtuid1");
		List<KeyValuePair> vals2 = worker.getReduceResults("mrtuid2");
		assertTrue(vals1.contains(new KeyValuePair("key", "value")));
		assertTrue(vals2.contains(new KeyValuePair("key2", "value2")));
		assertTrue(vals1.size() == 1);
		assertTrue(vals2.size() == 1);
	}

	@Test
	public void shouldNotMixReduceResults() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeReduceResult("mrtuid", new KeyValuePair("key", "value"));
		assertNull(worker.getMapResults("mrtuid"));
	}

	@Test
	public void shouldNotMixMapResults() {
		DeterministicExecutor exec = new DeterministicExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		worker.storeMapResult("mrtuid", new KeyValuePair("key", "value"));
		assertNull(worker.getReduceResults("mrtuid"));
	}


}
