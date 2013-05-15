package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
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

import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.impl.InMemoryPersistence;
import ch.zhaw.mapreduce.plugins.socket.AbstractMapReduceMasterSocketTest;
import ch.zhaw.mapreduce.plugins.socket.ResultState;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;
import ch.zhaw.mapreduce.plugins.socket.SocketResultObserver;

public class SocketResultCollectorImplTest extends AbstractMapReduceMasterSocketTest {

	@Test
	public void shouldNotifySocketWorkerWithSuccess() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.requestedBy(srObserver));
		mockery.checking(new Expectations() {
			{
				atLeast(1).of(saRes).getMapReduceTaskUuid();
				will(returnValue(mrUuid));
				atLeast(1).of(saRes).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(saRes).getResult();
				will(returnValue(Collections.emptyList()));
				atLeast(1).of(saRes).wasSuccessful();
				will(returnValue(true));
				oneOf(srObserver).resultAvailable(mrUuid, taskUuid, true);
			}
		});
		coll.pushResult(saRes);
	}

	@Test
	public void shouldNotifySocketWorkerWithFailure() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.requestedBy(srObserver));
		mockery.checking(new Expectations() {
			{
				atLeast(1).of(saRes).getMapReduceTaskUuid();
				will(returnValue(mrUuid));
				atLeast(1).of(saRes).getTaskUuid();
				will(returnValue(taskUuid));
				atLeast(1).of(saRes).wasSuccessful();
				will(returnValue(false));
				oneOf(saRes).getException();
				will(returnValue(new Exception()));
				oneOf(srObserver).resultAvailable(mrUuid, taskUuid, false);
			}
		});
		coll.pushResult(saRes);
	}

	@Test
	public void shouldAddObserverToList() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.registerObserver(mrUuid, taskUuid, srObserver);
		ConcurrentMap<String, ResultState> results = coll.getResultStates();
		String key = coll.createKey(mrUuid, taskUuid);
		assertTrue(results.containsKey(key));
		assertTrue(results.get(key).requested());
		assertSame(results.get(key).requestedBy(), srObserver);
	}

	@Test
	public void shouldReturnSuccessIfResultIsAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.resultAvailable(true));
		assertTrue(coll.registerObserver(mrUuid, taskUuid, srObserver));
	}

	@Test
	public void shouldReturnFailureIfResultIsAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.resultAvailable(true));
		assertTrue(coll.registerObserver(mrUuid, taskUuid, srObserver));
	}

	@Test
	public void shouldReturnNullIfResultIsNotAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		assertNull(coll.registerObserver(mrUuid, taskUuid, srObserver));
	}

	@Test
	public void shouldBeThreadSafe() throws Exception {
		Persistence pers = new InMemoryPersistence();
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(pers);
		ExecutorService service = Executors.newFixedThreadPool(10);

		Logger.getLogger("ch.zhaw").setLevel(Level.SEVERE);
		service.invokeAll(createPusherTasks(coll, 1000, 1000, false, 0));
		service.invokeAll(createPullerTasks(coll, 1000, 1000, true, 0));
		service.invokeAll(createPusherTasks(coll, 1000, 1000, true, 100000));
		service.invokeAll(createPullerTasks(coll, 1000, 1000, false, 100000));
		service.invokeAll(createPusherTasks(coll, 1000, 1000, false, 200000));
		service.invokeAll(createPullerTasks(coll, 1000, 1000, true, 200000));
		service.invokeAll(createPusherTasks(coll, 1000, 1000, true, 1000000));
		service.invokeAll(createPullerTasks(coll, 1000, 1000, false, 1000000));
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
			j = ntasks-1;
		} else {
			j = 0;
		}

		while (true) {
			if (reversed) { if ( j < 0) { break; } }
			else { if (j >= ntasks) { break; } }
			final int j2 = j;
			tasks.add(new Callable<Void>() {
				@Override
				public Void call() throws Exception {
					for (int i = 0; i < ncalls; i++) {
						if (i % 2 == 0) {
							coll.pushResult(new SocketAgentResultImpl("mrUuid" + j2, "task" + (i + idxoffset), new RuntimeException()));
						} else {
							coll.pushResult(new SocketAgentResultImpl("mrUuid" + j2, "task" + (i + idxoffset), new ArrayList<String>()));
						}
					}
					return null;
				};
			});
			if (reversed) j--;
			else j++;
		}
		return tasks;
	}

	private static List<Callable<List<TestSocketResultObserver>>> createPullerTasks(final SocketResultCollector coll,
			int ntasks, final int ncalls, boolean reversed, final int idxoffset) {
		List<Callable<List<TestSocketResultObserver>>> tasks = new ArrayList<Callable<List<TestSocketResultObserver>>>(
				ntasks);
		int j;
		if (reversed) {
			j = ntasks-1;
		} else {
			j = 0;
		}

		while (true) {
			if (reversed) { if ( j < 0) { break; } }
			else { if (j >= ntasks) { break; } }
			final int j2 = j;
			tasks.add(new Callable<List<TestSocketResultObserver>>() {
				@Override
				public List<TestSocketResultObserver> call() throws Exception {
					List<TestSocketResultObserver> observers = new ArrayList<TestSocketResultObserver>(ncalls);
					for (int i = 0; i < ncalls; i++) {
						String mruuid = "mrUuid" + j2;
						String taskid = "task" + (i + idxoffset);
						TestSocketResultObserver observer = new TestSocketResultObserver(mruuid, taskid, null);
						Boolean success = coll.registerObserver(mruuid, taskid, observer);
						if (success != null) {
							observer.success = success;
						}
						observers.add(observer);
					}
					return observers;
				};
			});
			if (reversed) j--;
			else j++;
		}
		return tasks;
	}

	private static class TestSocketResultObserver implements SocketResultObserver {

		private final String mrUuid;

		private final String taskid;

		Boolean success;

		public TestSocketResultObserver(String mrUuid, String taskid, Boolean success) {
			this.mrUuid = mrUuid;
			this.taskid = taskid;
			this.success = success;
		}

		@Override
		public void resultAvailable(String mapReduceTaskUuid, String taskUuid, boolean success) {
			if (!this.mrUuid.equals(mapReduceTaskUuid) || this.taskid.equals(taskUuid)) {
				throw new IllegalArgumentException("ids do not match");
			} else if (this.success != null) {
				throw new IllegalStateException("Success was already set");
			}
			this.success = success;
		}

	}

}
