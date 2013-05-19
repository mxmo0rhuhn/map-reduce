package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AbstractMapReduceMasterSocketTest;
import ch.zhaw.mapreduce.plugins.socket.ResultState;
import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;
import ch.zhaw.mapreduce.plugins.socket.SocketResultObserver;

public class SocketResultCollectorImplTest extends AbstractMapReduceMasterSocketTest {

	@Test
	public void shouldNotifySocketWorkerWithResult() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl();
		coll.getResultStates().put(taskUuid, ResultState.requestedBy(srObserver));
		mockery.checking(new Expectations() {
			{
				atLeast(1).of(saRes).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(srObserver).resultAvailable(taskUuid, saRes);
			}
		});
		coll.pushResult(saRes);
	}

	@Test
	public void shouldNotifySocketWorkerWithFailure() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl();
		coll.getResultStates().put(taskUuid, ResultState.requestedBy(srObserver));
		mockery.checking(new Expectations() {
			{
				atLeast(1).of(saRes).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(srObserver).resultAvailable(taskUuid, saRes);
			}
		});
		coll.pushResult(saRes);
	}

	@Test
	public void shouldAddObserverToList() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl();
		coll.registerObserver(taskUuid, srObserver);
		ConcurrentMap<String, ResultState> results = coll.getResultStates();
		assertTrue(results.containsKey(taskUuid));
		assertTrue(results.get(taskUuid).requested());
		assertSame(results.get(taskUuid).requestedBy(), srObserver);
	}

	@Test
	public void shouldReturnSuccessIfResultIsAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl();
		coll.getResultStates().put(taskUuid, ResultState.resultAvailable(saRes));
		assertEquals(saRes, coll.registerObserver(taskUuid, srObserver));
	}

	@Test
	public void shouldReturnFailureIfResultIsAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl();
		coll.getResultStates().put(taskUuid, ResultState.resultAvailable(saRes));
		assertEquals(saRes, coll.registerObserver(taskUuid, srObserver));
	}

	@Test
	public void shouldReturnNullIfResultIsNotAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl();
		assertNull(coll.registerObserver(taskUuid, srObserver));
	}

	@Test
	public void shouldBeThreadSafe() throws Exception {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl();
		ExecutorService service = Executors.newFixedThreadPool(10);

		Logger.getLogger("ch.zhaw").setLevel(Level.SEVERE);
		service.invokeAll(createPusherTasks(coll, 100, 1000, false, 0));
		service.invokeAll(createPullerTasks(coll, 100, 1000, true, 0));
		service.invokeAll(createPusherTasks(coll, 100, 1000, true, 100000));
		service.invokeAll(createPullerTasks(coll, 100, 1000, false, 100000));
		service.invokeAll(createPusherTasks(coll, 100, 1000, false, 200000));
		service.invokeAll(createPullerTasks(coll, 100, 1000, true, 200000));
		service.invokeAll(createPusherTasks(coll, 100, 1000, true, 1000000));
		service.invokeAll(createPullerTasks(coll, 100, 1000, false, 1000000));
		service.shutdown();
		assertTrue("Not finished in time", service.awaitTermination(10, TimeUnit.SECONDS));
		for (Entry<String, ResultState> entry : coll.getResultStates().entrySet()) {
			System.out.println(entry);
		}
		assertTrue("Results are left over", coll.getResultStates().isEmpty());
		service.shutdownNow();
	}

	private static List<Callable<Void>> createPusherTasks(final SocketResultCollector coll, int ntasks,
			final int ncalls, boolean reversed, final int idxoffset) {
		List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
		int j;
		if (reversed) {
			j = ntasks - 1;
		} else {
			j = 0;
		}

		while (true) {
			if (reversed) {
				if (j < 0) {
					break;
				}
			} else {
				if (j >= ntasks) {
					break;
				}
			}
			final int j2 = j;
			tasks.add(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					for (int i = 0; i < ncalls; i++) {
						String taskid = "task" + j2 + "-" + (i + idxoffset);
						if (i % 2 == 0) {
							coll.pushResult(new SocketAgentResultImpl(taskid, new RuntimeException()));
						} else {
							coll.pushResult(new SocketAgentResultImpl(taskid, new ArrayList<String>()));
						}
					}
					return null;
				};
			});
			if (reversed)
				j--;
			else
				j++;
		}
		return tasks;
	}

	private static List<Callable<List<TestSocketResultObserver>>> createPullerTasks(final SocketResultCollector coll,
			int ntasks, final int ncalls, boolean reversed, final int idxoffset) {
		List<Callable<List<TestSocketResultObserver>>> tasks = new ArrayList<Callable<List<TestSocketResultObserver>>>(
				ntasks);
		int j;
		if (reversed) {
			j = ntasks - 1;
		} else {
			j = 0;
		}

		while (true) {
			if (reversed) {
				if (j < 0) {
					break;
				}
			} else {
				if (j >= ntasks) {
					break;
				}
			}
			final int j2 = j;
			tasks.add(new Callable<List<TestSocketResultObserver>>() {
				@Override
				public List<TestSocketResultObserver> call() throws Exception {
					List<TestSocketResultObserver> observers = new ArrayList<TestSocketResultObserver>(ncalls);
					for (int i = 0; i < ncalls; i++) {
						String taskid = "task" + j2 + "-" + (i + idxoffset);
						TestSocketResultObserver observer = new TestSocketResultObserver(taskid, null);
						SocketAgentResult result = coll.registerObserver(taskid, observer);
						if (result != null) {
							observer.result = result;
						}
						observers.add(observer);
					}
					return observers;
				};
			});
			if (reversed)
				j--;
			else
				j++;
		}
		return tasks;
	}

	private static class TestSocketResultObserver implements SocketResultObserver {

		private final String taskid;

		SocketAgentResult result;

		public TestSocketResultObserver(String taskid, SocketAgentResult result) {
			this.taskid = taskid;
			this.result = result;
		}

		@Override
		public void resultAvailable(String taskUuid, SocketAgentResult result) {
			if (this.taskid.equals(taskUuid)) {
				throw new IllegalArgumentException("ids do not match");
			} else if (this.result != null) {
				throw new IllegalStateException("Result was already set");
			}
			this.result = result;
		}

	}

}
