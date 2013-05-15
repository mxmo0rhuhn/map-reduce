package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.auto.Auto;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.ReduceEmitter;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask.State;
import ch.zhaw.mapreduce.plugins.thread.ThreadWorker;


public class ReduceWorkerTaskTest {
	

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery() {
		{
			setThreadingPolicy(new Synchroniser());
		}
	};

	@Auto
	private Sequence events;

	@Mock
	private ReduceInstruction reduceInstr;

	@Mock
	private Context ctx;

	@Mock
	private ContextFactory ctxFactory;

	@Mock
	private ExecutorService execMock;
	
	@Mock
	private Persistence pers;

	private final String taskUUID = "taskUUID";
	
	private final String key = "key";

	private final List<KeyValuePair> keyVals = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1"), new KeyValuePair("key2", "val2")});

	@Test
	public void shouldSetMapReduceTaskUUID() {
		ReduceWorkerTask task = new ReduceWorkerTask("uuid", taskUUID, pers, reduceInstr, key, keyVals);
		assertEquals("uuid", task.getMapReduceTaskUuid());
	}

	@Test
	public void shouldSetReduceInstruction() {
		ReduceWorkerTask task = new ReduceWorkerTask("uuid", taskUUID, pers, reduceInstr, key, keyVals);
		assertSame(reduceInstr, task.getReduceInstruction());
	}

	@Test
	public void shouldRunReduceInstruction() {
		Executor poolExec = Executors.newSingleThreadExecutor();
		Pool pool = new Pool(poolExec, execMock, 1000);
		pool.init();
		final ReduceWorkerTask task = new ReduceWorkerTask("uuid", taskUUID, pers, reduceInstr, key, keyVals);
		this.mockery.checking(new Expectations() {
			{
				oneOf(reduceInstr).reduce(with(ctx), with(key), with(aNonNull(Iterator.class)));
			}
		});
		task.runTask(ctx);
	}

	@Test
	public void shouldSetInputUUID() {
		ReduceWorkerTask task = new ReduceWorkerTask("uuid", taskUUID, pers, reduceInstr, key, keyVals);
		assertEquals(taskUUID, task.getTaskUuid());
	}

	@Test
	public void shouldSetStateToFailedOnException() {
		ReduceWorkerTask task = new ReduceWorkerTask("uuid", taskUUID, pers, reduceInstr, key, keyVals);
		this.mockery.checking(new Expectations() {
			{
				oneOf(reduceInstr).reduce(with(ctx), with(key), with(aNonNull(Iterator.class)));
				will(throwException(new NullPointerException()));
				oneOf(pers).destroy("uuid", taskUUID);
			}
		});
		task.runTask(ctx);
		assertEquals(State.FAILED, task.getCurrentState());
	}

	@Test
	public void shouldSetStateToCompletedOnSuccess() {
		ReduceWorkerTask task = new ReduceWorkerTask("uuid", taskUUID, pers, reduceInstr, key, keyVals);
		this.mockery.checking(new Expectations() {
			{
				oneOf(reduceInstr).reduce(with(ctx), with(key), with(aNonNull(Iterator.class)));
			}
		});
		task.runTask(ctx);
		assertEquals(State.COMPLETED, task.getCurrentState());
	}

	@Test
	public void shouldSetStateToInitiatedInitially() {
		ReduceWorkerTask task = new ReduceWorkerTask("uuid", taskUUID, pers, reduceInstr, key, keyVals);
		assertEquals(State.INITIATED, task.getCurrentState());
	}

	@Test
	public void shouldBeInProgressWhileRunning() throws InterruptedException, BrokenBarrierException {
		ExecutorService poolExec = Executors.newSingleThreadExecutor();
		final CyclicBarrier barrier = new CyclicBarrier(2);
		ExecutorService taskExec = Executors.newSingleThreadExecutor();
		final Pool pool = new Pool(poolExec, execMock, 1000);
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, taskExec, ctxFactory);
		pool.donateWorker(worker);
		final ReduceWorkerTask task = new ReduceWorkerTask("mrtUuid", taskUUID,  pers, new ReduceInstruction() {
			@Override
			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
				try {
					barrier.await();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}

		}, key, keyVals);
		this.mockery.checking(new Expectations() {
			{
				oneOf(ctxFactory).createContext("mrtUuid", taskUUID);
				will(returnValue(ctx));
			}
		});
		pool.enqueueTask(task);
		Thread.yield();
		Thread.sleep(200);
		assertEquals(State.INPROGRESS, task.getCurrentState());
		try {
			barrier.await(100, TimeUnit.MILLISECONDS);
		} catch (TimeoutException te) {
			fail("should return immediately");
		}
	}

	@Test
	public void shouldBeAbleToRerunTests() {
		ExecutorService poolExec = Executors.newSingleThreadExecutor();
		ExactCommandExecutor threadExec1 = new ExactCommandExecutor(1);
		ExactCommandExecutor threadExec2 = new ExactCommandExecutor(1);
		final Pool pool = new Pool(poolExec, execMock, 1000);
		final AtomicInteger cnt = new AtomicInteger();
		pool.init();
		ThreadWorker worker1 = new ThreadWorker(pool, threadExec1, ctxFactory);
		pool.donateWorker(worker1);
		ThreadWorker worker2 = new ThreadWorker(pool, threadExec2, ctxFactory);
		pool.donateWorker(worker2);
		final ReduceWorkerTask task = new ReduceWorkerTask("mrtUuid", taskUUID, pers, new ReduceInstruction() {
			
			@Override
			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
				if (cnt.get() == 0) {
					cnt.incrementAndGet();
					throw new NullPointerException();
				} else if (cnt.get() == 1) {
					// successful
				} else {
					throw new NullPointerException();
				}
			}
		}, key, keyVals);
		this.mockery.checking(new Expectations() {
			{
				oneOf(ctxFactory).createContext("mrtUuid", taskUUID);
				will(returnValue(ctx));
				inSequence(events);
				oneOf(pers).destroy("mrtUuid", taskUUID);
				inSequence(events);
				oneOf(ctxFactory).createContext("mrtUuid", taskUUID);
				will(returnValue(ctx));
			}
		});
		pool.enqueueTask(task);
		assertTrue(threadExec1.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.FAILED, task.getCurrentState());
		pool.enqueueTask(task);
		assertTrue(threadExec2.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.COMPLETED, task.getCurrentState());
	}

	@Test
	public void shouldBeEnqueuedAfterSubmissionToPool() {
		Pool pool = new Pool(Executors.newSingleThreadExecutor(), execMock, 1000);
		pool.init();
		final ReduceWorkerTask task = new ReduceWorkerTask("mrtuid", taskUUID, pers, reduceInstr, key, keyVals);
		this.mockery.checking(new Expectations() {
			{
				never(reduceInstr);
			}
		});
		pool.enqueueTask(task);
		assertEquals(State.ENQUEUED, task.getCurrentState());
	}

}