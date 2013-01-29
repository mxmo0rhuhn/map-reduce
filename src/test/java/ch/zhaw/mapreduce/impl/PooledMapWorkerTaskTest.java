package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.WorkerTask.State;
import ch.zhaw.mapreduce.workers.ThreadWorker;

@RunWith(JMock.class)
public class PooledMapWorkerTaskTest {

	private Mockery context;

	private Pool p;

	private MapInstruction mapInstr;

	private CombinerInstruction combInstr;

	private String inputUUID;

	private String input;

	@Before
	public void initMock() {
		this.context = new JUnit4Mockery();
		this.p = this.context.mock(Pool.class);
		this.mapInstr = this.context.mock(MapInstruction.class);
		this.combInstr = this.context.mock(CombinerInstruction.class);
		this.inputUUID = "inputUUID";
		this.input = "hello";
	}

	@Test
	public void shouldSetMapReduceTaskUUID() {
		MapWorkerTask task = new MapWorkerTask(p, "uuid", mapInstr, combInstr, inputUUID, input);
		assertEquals("uuid", task.getMapReduceTaskUUID());
	}

	@Test
	public void shouldSetMapInstruction() {
		MapWorkerTask task = new MapWorkerTask(p, "uuid", mapInstr, combInstr, inputUUID, input);
		assertSame(mapInstr, task.getMapInstruction());
	}

	@Test
	public void shouldSetCombinerInstruction() {
		MapWorkerTask task = new MapWorkerTask(p, "uuid", mapInstr, combInstr, inputUUID, input);
		assertSame(combInstr, task.getCombinerInstruction());
	}

	@Test
	public void shouldCopeWithNullCombiner() {
		MapWorkerTask task = new MapWorkerTask(p, "uuid", mapInstr, null, inputUUID, input);
		assertNull(task.getCombinerInstruction());
	}

	@Test
	public void shouldEmitUnderMapReduceTaskUUID() {
		Executor poolExec = Executors.newSingleThreadExecutor();
		LocalThreadPool pool = new LocalThreadPool(poolExec);
		pool.init();
		final MapWorkerTask task = new MapWorkerTask(pool, "mrtUuid", new MapInstruction() {
			@Override
			public void map(MapEmitter emitter, String toDo) {
				for (String part : toDo.split(" ")) {
					emitter.emitIntermediateMapResult(part, "1");
				}
			}
		}, null, inputUUID, input);
		ExactCommandExecutor threadExec = new ExactCommandExecutor(1);
		ThreadWorker worker = new ThreadWorker(pool, threadExec);
		pool.donateWorker(worker);
		task.runMapTask();
		assertTrue(threadExec.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		List<KeyValuePair> vals = worker.getMapResults("mrtUuid");
		assertTrue(vals.contains(new KeyValuePair("hello", "1")));
		assertEquals(1, vals.size());
	}

	@Test
	public void shouldSetInputUUID() {
		final MapWorkerTask task = new MapWorkerTask(p, "mrtUuid", mapInstr, combInstr, inputUUID, input);
		this.context.checking(new Expectations() {
			{
				oneOf(p).enqueueWork(task);
			}
		});
		task.runMapTask();
		assertEquals("inputUUID", task.getUUID());
	}

	@Test
	public void shouldSetStateToFailedOnException() {
		Executor poolExec = Executors.newSingleThreadExecutor();
		ExactCommandExecutor taskExec = new ExactCommandExecutor(1);
		final LocalThreadPool pool = new LocalThreadPool(poolExec);
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, taskExec);
		pool.donateWorker(worker);
		final MapWorkerTask task = new MapWorkerTask(pool, "mrtUuid", new MapInstruction() {

			@Override
			public void map(MapEmitter emitter, String toDo) {
				throw new NullPointerException();
			}
		}, combInstr, inputUUID, input);
		task.runMapTask();
		assertTrue(taskExec.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.FAILED, task.getCurrentState());
		assertNull(task.getWorker());
	}

	@Test
	public void shouldSetStateToCompletedOnSuccess() {
		Executor poolExec = Executors.newSingleThreadExecutor();
		ExactCommandExecutor taskExec = new ExactCommandExecutor(1);
		final LocalThreadPool pool = new LocalThreadPool(poolExec);
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, taskExec);
		pool.donateWorker(worker);
		final MapWorkerTask task = new MapWorkerTask(pool, "mrtUuid", new MapInstruction() {

			@Override
			public void map(MapEmitter emitter, String toDo) {
				for (String part : toDo.split(" ")) {
					emitter.emitIntermediateMapResult(part, "1");
				}
			}
		}, combInstr, inputUUID, input);
		this.context.checking(new Expectations() {
			{
				oneOf(combInstr).combine(with(aNonNull(Iterator.class)));
			}
		});
		task.runMapTask();
		assertTrue(taskExec.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.COMPLETED, task.getCurrentState());
		assertSame(worker, task.getWorker());
	}
	
	@Test
	public void shouldBeCompletedWithoutEmittingDuringMap() {
		Executor poolExec = Executors.newSingleThreadExecutor();
		ExactCommandExecutor taskExec = new ExactCommandExecutor(1);
		final LocalThreadPool pool = new LocalThreadPool(poolExec);
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, taskExec);
		pool.donateWorker(worker);
		final MapWorkerTask task = new MapWorkerTask(pool, "mrtUuid", new MapInstruction() {
			@Override public void map(MapEmitter emitter, String toDo) { }
		}, combInstr, inputUUID, input);
		this.context.checking(new Expectations() {
			{
				oneOf(combInstr).combine(with(aNonNull(Iterator.class)));
			}
		});
		task.runMapTask();
		assertTrue(taskExec.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.COMPLETED, task.getCurrentState());
		assertSame(worker, task.getWorker());
	}

	@Test
	public void shouldSetStateToInitiatedInitially() {
		MapWorkerTask task = new MapWorkerTask(p, "mrtUuid", mapInstr, combInstr, inputUUID, input);
		assertEquals(State.INITIATED, task.getCurrentState());
	}

	@Test
	public void shouldBeInProgressWhileRunning() throws InterruptedException, BrokenBarrierException {
		Executor poolExec = Executors.newSingleThreadExecutor();
		final CyclicBarrier barrier = new CyclicBarrier(2);
		Executor taskExec = Executors.newSingleThreadExecutor();
		final LocalThreadPool pool = new LocalThreadPool(poolExec);
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, taskExec);
		pool.donateWorker(worker);
		final MapWorkerTask task = new MapWorkerTask(pool, "mrtUuid", new MapInstruction() {

			@Override
			public void map(MapEmitter emitter, String toDo) {
				try {
					barrier.await();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		}, null, inputUUID, input);
		task.runMapTask();
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
		Executor poolExec = Executors.newSingleThreadExecutor();
		ExactCommandExecutor threadExec1 = new ExactCommandExecutor(1);
		ExactCommandExecutor threadExec2 = new ExactCommandExecutor(1);
		final LocalThreadPool pool = new LocalThreadPool(poolExec);
		final AtomicInteger cnt = new AtomicInteger();
		pool.init();
		ThreadWorker worker1 = new ThreadWorker(pool, threadExec1);
		pool.donateWorker(worker1);
		ThreadWorker worker2 = new ThreadWorker(pool, threadExec2);
		pool.donateWorker(worker2);
		final MapWorkerTask task = new MapWorkerTask(pool, "mrtUuid", new MapInstruction() {

			@Override
			public void map(MapEmitter emitter, String toDo) {
				if (cnt.get() == 0) {
					cnt.incrementAndGet();
					throw new NullPointerException();
				} else if (cnt.get() == 1) {
					// successful
				} else {
					throw new NullPointerException();
				}
			}
		}, null, inputUUID, input);
		task.runMapTask();
		assertTrue(threadExec1.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.FAILED, task.getCurrentState());
		assertNull(task.getWorker());
		task.runMapTask();
		assertTrue(threadExec2.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.COMPLETED, task.getCurrentState());
		assertSame(worker2, task.getWorker());
	}

	@Test
	public void shouldBeEnqueuedAfterSubmissionToPool() {
		final MapWorkerTask task = new MapWorkerTask(p, "mrtuid", mapInstr, combInstr, inputUUID, input);
		this.context.checking(new Expectations() {
			{
				oneOf(p).enqueueWork(task);
			}
		});
		task.runMapTask();
		assertEquals(State.ENQUEUED, task.getCurrentState());
	}
	
	@Test
	public void shouldCombineAfterTask() {
		final Executor poolExec = Executors.newSingleThreadExecutor();
		final ExactCommandExecutor threadExec = new ExactCommandExecutor(1);
		final LocalThreadPool pool = new LocalThreadPool(poolExec);
		final ThreadWorker worker = new ThreadWorker(pool, threadExec);
		final MapWorkerTask task = new MapWorkerTask(pool, "mrtuid", new WordCounterMapper(), new WordCounterCombiner(),
				"inputUUID", "foo bar foo");
		pool.init();
		pool.donateWorker(worker);
		task.runMapTask();
		Thread.yield();
		assertTrue(threadExec.waitForExpectedTasks(300, TimeUnit.MILLISECONDS));
		List<KeyValuePair> res = worker.getMapResults("mrtuid");
		assertTrue(res.contains(new KeyValuePair("foo", "1 1")));
		assertTrue(res.contains(new KeyValuePair("bar", "1")));
	}

}

class WordCounterMapper implements MapInstruction {
	@Override
	public void map(MapEmitter emitter, String toDo) {
		for (String word : toDo.trim().split(" ")) {
			emitter.emitIntermediateMapResult(word.trim(), "1");
		}
	}
}

class WordCounterCombiner implements CombinerInstruction {
	@Override
	public List<KeyValuePair> combine(Iterator<KeyValuePair> toCombine) {
		Map<String, KeyValuePair> combined = new HashMap<String, KeyValuePair>();
		while (toCombine.hasNext()) {
			KeyValuePair val = toCombine.next();
			if (!combined.containsKey(val.getKey())) {
				combined.put(val.getKey(), val);
			} else {
				KeyValuePair newVal = new KeyValuePair(val.getKey(), combined.get(val.getKey()).getValue() + ' '
						+ val.getValue());
				combined.put(val.getKey(), newVal);
			}
		}
		return new ArrayList<KeyValuePair>(combined.values());
	}
}