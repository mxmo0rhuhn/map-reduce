package ch.zhaw.mapreduce.impl;



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.ReduceEmitter;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.WorkerTask.State;
import ch.zhaw.mapreduce.workers.ThreadWorker;

public class ReduceWorkerTaskTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();

	@Mock
	private ReduceInstruction redInstr;

	private List<KeyValuePair> keyVals = Arrays.asList(new KeyValuePair[] { new KeyValuePair("hello", "1"), new KeyValuePair("foo", "1"),
				new KeyValuePair("hello", "2") });

	@Test
	public void shouldSetMrUuid() {
		ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", redInstr, keyVals);
		assertEquals("mruid", task.getMapReduceTaskUUID());
	}

	@Test
	public void shouldSetReduceInstruction() {
		ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", redInstr, keyVals);
		assertSame(redInstr, task.getReduceTask());
	}

	@Test
	public void shouldBeInitiatedInitially() {
		ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", redInstr, keyVals);
		assertEquals(State.INITIATED, task.getCurrentState());
	}

	@Test
	public void shouldBeEnqueuedAfterSubmissionToPool() {
		final ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", redInstr, keyVals);
		this.mockery.checking(new Expectations() {
			{
				oneOf(p).enqueueWork(task);
			}
		});
		task.runReduceTask();
		assertEquals(State.ENQUEUED, task.getCurrentState());
	}

	@Test
	public void shouldBeCompletedAfterSuccessfulExecution() {
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		ThreadWorker worker = new ThreadWorker(pool, exec);
		pool.donateWorker(worker);
		final ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", redInstr, keyVals);
		this.context.checking(new Expectations() {
			{
				oneOf(redInstr).reduce(with(task), with("key"), with(aNonNull(Iterator.class)));
			}
		});
		task.runReduceTask();
		assertTrue(exec.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.COMPLETED, task.getCurrentState());
	}

	@Test
	public void shouldBeInProgressWhileExecuting() throws InterruptedException, BrokenBarrierException {
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		Executor exec = Executors.newSingleThreadExecutor();
		final CyclicBarrier barrier = new CyclicBarrier(2);
		ThreadWorker worker = new ThreadWorker(pool, exec);
		assertTrue(pool.donateWorker(worker));
		ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", new ReduceInstruction() {

			@Override
			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
				try {
					barrier.await();
				} catch (Exception e) {
					throw new RuntimeException();
				}
			}
		}, keyVals);
		task.runReduceTask();
		Thread.yield();
		Thread.sleep(200);
		assertEquals(State.INPROGRESS, task.getCurrentState());
	}
	
	@Test
	public void shouldUseKeyAsUUID() {
		ReduceWorkerTask task = new ReduceWorkerTask(p, "mrtuid", "key", redInstr, keyVals);
		assertEquals("key", task.getUUID());
	}

	@Test
	public void shouldBeFailedAfterException() throws InterruptedException {
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		Executor exec = Executors.newSingleThreadExecutor();
		ThreadWorker worker = new ThreadWorker(pool, exec);
		assertTrue(pool.donateWorker(worker));
		ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", new ReduceInstruction() {

			@Override
			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
				throw new NullPointerException();
			}
		}, keyVals);
		task.runReduceTask();
		Thread.yield();
		Thread.sleep(200);
		assertEquals(State.FAILED, task.getCurrentState());
	}

	@Test
	public void shouldBeAbleToRerunFailed() {
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ExactCommandExecutor exec1 = new ExactCommandExecutor(1);
		ExactCommandExecutor exec2 = new ExactCommandExecutor(1);
		ThreadWorker worker1 = new ThreadWorker(pool, exec1);
		ThreadWorker worker2 = new ThreadWorker(pool, exec2);
		assertTrue(pool.donateWorker(worker1));
		assertTrue(pool.donateWorker(worker2));
		final AtomicInteger cnt = new AtomicInteger();
		ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", new ReduceInstruction() {

			@Override
			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
				if (cnt.get() == 0) {
					cnt.incrementAndGet();
					throw new NullPointerException();
				} else if (cnt.get() == 1) {
					// all fine
				} else {
					throw new IllegalStateException("unexpected");
				}
			}
		}, keyVals);
		task.runReduceTask();
		assertTrue(exec1.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.FAILED, task.getCurrentState());
		assertNull(task.getWorker());
		
		task.runReduceTask();
		assertTrue(exec2.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.COMPLETED, task.getCurrentState());
		assertSame(worker2, task.getWorker());
	}
	
	@Test
	public void shouldEmitToProcessingWorker() {
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		ThreadWorker worker = new ThreadWorker(pool, exec);
		assertTrue(pool.donateWorker(worker));
		ReduceWorkerTask task = new ReduceWorkerTask("mruid", "key", new ReduceInstruction() {

			@Override
			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
				emitter.emit("value");
			}
		}, keyVals);
		assertTrue(task.runReduceTask());
		assertTrue(exec.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		List<KeyValuePair> results = worker.getReduceResults("mruid");
		assertEquals(1, results.size());
		assertEquals(new KeyValuePair("key", "value"), results.get(0));
	}
	

}
