package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Provider;

import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.auto.Auto;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.WorkerTask.State;
import ch.zhaw.mapreduce.plugins.thread.ThreadWorker;


public class MapWorkerTaskTest {
	

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery() {
		{
			setThreadingPolicy(new Synchroniser());
		}
	};

	@Auto
	private Sequence events;
	
	@Mock
	private MapInstruction mapInstr;

	@Mock
	private CombinerInstruction combInstr;

	@Mock
	private Context ctx;

	@Mock
	private Provider<Context> ctxProvider;

	@Mock
	private ExecutorService execMock;
	
	@Mock
	private Persistence pers;
	
	private String inputUUID = "inputUUID";

	private String input = "hello";

	@Test
	public void shouldSetMapInstruction() {
		MapWorkerTask task = new MapWorkerTask(inputUUID, pers, mapInstr, combInstr, input);
		assertSame(mapInstr, task.getMapInstruction());
	}

	@Test
	public void shouldSetCombinerInstruction() {
		MapWorkerTask task = new MapWorkerTask(inputUUID, pers, mapInstr, combInstr, input);
		assertSame(combInstr, task.getCombinerInstruction());
	}

	@Test
	public void shouldCopeWithNullCombiner() {
		MapWorkerTask task = new MapWorkerTask(inputUUID, pers, mapInstr, null, input);
		assertNull(task.getCombinerInstruction());
	}

	@Test
	public void shouldRunMapInstruction() {
		final MapWorkerTask task = new MapWorkerTask(inputUUID, pers, new MapInstruction() {
			@Override
			public void map(MapEmitter emitter, String toDo) {
				for (String part : toDo.split(" ")) {
					emitter.emitIntermediateMapResult(part, "1");
				}
			}
		}, null, input);
		this.mockery.checking(new Expectations() {
			{
				oneOf(ctx).emitIntermediateMapResult("hello", "1");
			}
		});
		task.runTask(ctx);
	}

	@Test
	public void shouldSetInputUUID() {
		final MapWorkerTask task = new MapWorkerTask(inputUUID, pers, mapInstr, combInstr, input);
		assertEquals(inputUUID, task.getTaskUuid());
	}

	@Test
	public void shouldSetStateToInitiatedInitially() {
		MapWorkerTask task = new MapWorkerTask(inputUUID, pers, mapInstr, combInstr, input);
		assertEquals(State.INITIATED, task.getCurrentState());
	}

	@Test
	public void shouldBeInProgressWhileRunning() throws InterruptedException, BrokenBarrierException {
		final CyclicBarrier barrier = new CyclicBarrier(2);
		ExecutorService taskExec = Executors.newSingleThreadExecutor();
		final PoolImpl pool = new PoolImpl(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, taskExec, ctxProvider);
		pool.donateWorker(worker);
		final MapWorkerTask task = new MapWorkerTask(inputUUID, pers, new MapInstruction() {

			@Override
			public void map(MapEmitter emitter, String toDo) {
				try {
					barrier.await();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		}, null, input);
		this.mockery.checking(new Expectations() { {
				oneOf(ctxProvider).get(); will(returnValue(ctx));
			} });
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
	public void shouldBeEnqueuedAfterSubmissionToPool() throws Exception {
		final PoolImpl pool = new PoolImpl(Executors.newSingleThreadExecutor());
		pool.init();
		final MapWorkerTask task = new MapWorkerTask(inputUUID, pers, mapInstr, null, input);
		this.mockery.checking(new Expectations() {
			{
				never(mapInstr);
			}
		});
		pool.enqueueTask(task);
		assertEquals(State.ENQUEUED, task.getCurrentState());
	}

	@Test
	public void shouldCombineAfterTask() {
		final MapWorkerTask task = new MapWorkerTask("inputUUID", pers, mapInstr, combInstr, "hello");
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[] { new KeyValuePair("hello", "1") });
		final List<KeyValuePair> combined = Arrays.asList(new KeyValuePair[] { new KeyValuePair("hello", "2") });
		this.mockery.checking(new Expectations() {
			{
				oneOf(mapInstr).map(ctx, "hello");
				oneOf(ctx).getMapResult();
				will(returnValue(result));
				// TODO check genauer iterator mit erwarteten werten, nicht einfach irgendeiner
				oneOf(combInstr).combine(with(aNonNull(Iterator.class)));
				will(returnValue(combined));
				oneOf(ctx).replaceMapResult(combined);
			}
		});
		task.runTask(ctx);
	}
	
	@Test
	public void shouldAlsoTransitionToCompleteIfResultIsEmpty() throws Exception {
		final MapWorkerTask task = new MapWorkerTask("inputUUID", pers, mapInstr, combInstr, "hello");
		task.successful(Collections.emptyList());
		assertEquals(State.COMPLETED, task.getCurrentState());
	}

	@Test
	public void shouldAlsoTransitionToCompleteIfResultIsNull() throws Exception {
		final MapWorkerTask task = new MapWorkerTask("inputUUID", pers, mapInstr, combInstr, "hello");
		task.successful(null);
		assertEquals(State.COMPLETED, task.getCurrentState());
	}
}