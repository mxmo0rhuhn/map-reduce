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
import org.jmock.Sequence;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask.State;
import ch.zhaw.mapreduce.plugins.thread.ThreadWorker;

@RunWith(JMock.class)
public class MapWorkerTaskTest {

	private Mockery context;

	private Pool p;

	private MapInstruction mapInstr;

	private CombinerInstruction combInstr;

	private String inputUUID;

	private String input;

	@Before
	public void initMock() {
		this.context = new JUnit4Mockery();
		this.p = new Pool(Executors.newSingleThreadExecutor());
		this.mapInstr = this.context.mock(MapInstruction.class);
		this.combInstr = this.context.mock(CombinerInstruction.class);
		this.inputUUID = "inputUUID";
		this.input = "hello";
	}

	@Test
	public void shouldSetMapReduceTaskUUID() {
		MapWorkerTask task = new MapWorkerTask("uuid", inputUUID, mapInstr, combInstr, input);
		assertEquals("uuid", task.getMapReduceTaskUUID());
	}

	@Test
	public void shouldSetMapInstruction() {
		MapWorkerTask task = new MapWorkerTask("uuid", inputUUID, mapInstr, combInstr, input);
		assertSame(mapInstr, task.getMapInstruction());
	}

	@Test
	public void shouldSetCombinerInstruction() {
		MapWorkerTask task = new MapWorkerTask("uuid", inputUUID, mapInstr, combInstr, input);
		assertSame(combInstr, task.getCombinerInstruction());
	}

	@Test
	public void shouldCopeWithNullCombiner() {
		MapWorkerTask task = new MapWorkerTask("uuid", inputUUID, mapInstr, null, input);
		assertNull(task.getCombinerInstruction());
	}

	@Test
	public void shouldRunMapInstruction() {
		final Context ctx = this.context.mock(Context.class);
		Executor poolExec = Executors.newSingleThreadExecutor();
		Pool pool = new Pool(poolExec);
		pool.init();
		final MapWorkerTask task = new MapWorkerTask("mrtUuid", inputUUID, new MapInstruction() {
			@Override
			public void map(MapEmitter emitter, String toDo) {
				for (String part : toDo.split(" ")) {
					emitter.emitIntermediateMapResult(part, "1");
				}
			}
		}, null, input);
		this.context.checking(new Expectations() {
			{
				oneOf(ctx).emitIntermediateMapResult("hello", "1");
			}
		});
		task.runTask(ctx);
	}

	@Test
	public void shouldSetInputUUID() {
		final MapWorkerTask task = new MapWorkerTask("mrtUuid", inputUUID, mapInstr, combInstr, input);
		assertEquals(inputUUID, task.getUUID());
	}

	@Test
	public void shouldSetStateToFailedOnException() {
		final MapWorkerTask task = new MapWorkerTask("mrtUuid", inputUUID, new MapInstruction() {

			@Override
			public void map(MapEmitter emitter, String toDo) {
				throw new NullPointerException();
			}
		}, combInstr, input);
		final Worker worker = this.context.mock(Worker.class);
		this.context.checking(new Expectations() {{ 
			oneOf(worker).cleanSpecificResult("mrtUuid", inputUUID);
		}});
		task.setWorker(worker);
		task.runTask(null);
		assertEquals(State.FAILED, task.getCurrentState());
		assertSame(worker, task.getWorker());
	}

	@Test
	public void shouldSetStateToCompletedOnSuccess() {
		final MapWorkerTask task = new MapWorkerTask("mrtUuid", inputUUID, new MapInstruction() {
			@Override
			public void map(MapEmitter emitter, String toDo) {
			}
		}, null, input);
		final Worker worker = this.context.mock(Worker.class);
		this.context.checking(new Expectations() {{ 
			never(worker);
		}});
		task.setWorker(worker);
		task.runTask(null);
		assertEquals(State.COMPLETED, task.getCurrentState());
	}

	@Test
	public void shouldSetStateToInitiatedInitially() {
		MapWorkerTask task = new MapWorkerTask("mrtUuid", inputUUID, mapInstr, combInstr, input);
		assertEquals(State.INITIATED, task.getCurrentState());
	}

	@Test
	public void shouldBeInProgressWhileRunning() throws InterruptedException, BrokenBarrierException {
		final ContextFactory ctxFactory = this.context.mock(ContextFactory.class);
		final Context ctx = this.context.mock(Context.class);
		Executor poolExec = Executors.newSingleThreadExecutor();
		final CyclicBarrier barrier = new CyclicBarrier(2);
		Executor taskExec = Executors.newSingleThreadExecutor();
		final Pool pool = new Pool(poolExec);
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, taskExec, ctxFactory);
		pool.donateWorker(worker);
		final MapWorkerTask task = new MapWorkerTask("mrtUuid", inputUUID, new MapInstruction() {

			@Override
			public void map(MapEmitter emitter, String toDo) {
				try {
					barrier.await();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		}, null, input);
		this.context.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext("mrtUuid", "inputUUID"); will(returnValue(ctx));
		}});
		pool.enqueueWork(task);
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
		final ContextFactory ctxFactory = this.context.mock(ContextFactory.class);
		final Context ctx = this.context.mock(Context.class);
		Executor poolExec = Executors.newSingleThreadExecutor();
		ExactCommandExecutor threadExec1 = new ExactCommandExecutor(1);
		ExactCommandExecutor threadExec2 = new ExactCommandExecutor(1);
		final Pool pool = new Pool(poolExec);
		final AtomicInteger cnt = new AtomicInteger();
		pool.init();
		ThreadWorker worker1 = new ThreadWorker(pool, threadExec1, ctxFactory);
		pool.donateWorker(worker1);
		ThreadWorker worker2 = new ThreadWorker(pool, threadExec2, ctxFactory);
		pool.donateWorker(worker2);
		final MapWorkerTask task = new MapWorkerTask("mrtUuid", inputUUID, new MapInstruction() {

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
		}, null, input);
		final Sequence seq = this.context.sequence("rerun");
		this.context.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext("mrtUuid", "inputUUID"); will(returnValue(ctx));
			inSequence(seq);
			oneOf(ctx).destroy();
			inSequence(seq);
			oneOf(ctxFactory).createContext("mrtUuid", "inputUUID"); will(returnValue(ctx));
		}});
		pool.enqueueWork(task);
		assertTrue(threadExec1.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.FAILED, task.getCurrentState());
		assertSame(worker1, task.getWorker());
		pool.enqueueWork(task);
		assertTrue(threadExec2.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
		assertEquals(State.COMPLETED, task.getCurrentState());
		assertSame(worker2, task.getWorker());
	}

	@Test
	public void shouldBeEnqueuedAfterSubmissionToPool() {
		final MapWorkerTask task = new MapWorkerTask("mrtuid", inputUUID, mapInstr, combInstr, input);
		this.context.checking(new Expectations() {
			{
				oneOf(p).enqueueWork(task);
			}
		});
		task.runTask(null);
		assertEquals(State.ENQUEUED, task.getCurrentState());
	}

	@Test
	public void shouldCombineAfterTask() {
		ContextFactory ctxFactory = this.context.mock(ContextFactory.class);
		final Executor poolExec = Executors.newSingleThreadExecutor();
		final ExactCommandExecutor threadExec = new ExactCommandExecutor(1);
		final Pool pool = new Pool(poolExec);
		final ThreadWorker worker = new ThreadWorker(pool, threadExec, ctxFactory);
		final MapWorkerTask task = new MapWorkerTask("mrtuid", "inputUUID", new WordCounterMapper(),
				new WordCounterCombiner(), "foo bar foo");
		pool.init();
		pool.donateWorker(worker);
		task.runTask(null);
		Thread.yield();
		assertTrue(threadExec.waitForExpectedTasks(300, TimeUnit.MILLISECONDS));
		List<KeyValuePair> res = worker.getMapResult("mrtuid", "iputUUID");
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
				combined.put((String) val.getKey(), val);
			} else {
				String old = (String) combined.get(val.getKey()).getValue();
				String new_ = (String) val.getValue();
				KeyValuePair newVal = new KeyValuePair(val.getKey(), old + ' ' + new_);
				combined.put((String) val.getKey(), newVal);
			}
		}
		return new ArrayList<KeyValuePair>(combined.values());
	}
}