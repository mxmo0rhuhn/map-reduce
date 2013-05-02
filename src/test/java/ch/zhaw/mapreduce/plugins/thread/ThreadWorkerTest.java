package ch.zhaw.mapreduce.plugins.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.auto.Auto;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.plugins.thread.ThreadWorker;

public class ThreadWorkerTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Auto
	Sequence events;
	
	@Mock
	private WorkerTask task;

	@Mock
	private ContextFactory ctxFactory;
	
	@Mock
	private Context ctx;
	
	@Test
	public void shouldGoBackToPool() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		final ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		this.mockery.checking(new Expectations() {{
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			}});
		worker.executeTask(task);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
		assertEquals(1, pool.getFreeWorkers());
	}
	
	@Test
	public void shouldReturnEmpyListForUnknownMapReduceIdForMapResult() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("hello", "1")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make another context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("anotherMrtUuid"));
			oneOf(task).getUUID(); will(returnValue("unknownTaskUuid"));
			oneOf(ctxFactory).createContext("anotherMrtUuid", "unknownTaskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		List<KeyValuePair> fromWorker = worker.getMapResult("mrtUuid", "taskUuid");
		assertNotNull(fromWorker);
		assertTrue(fromWorker.isEmpty());
	}
	
	@Test
	public void shouldReturnEmpyListForKnownMapReduceIdButUnknownTaskUuidForMapResult() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("hello", "1")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make another context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("unknownTaskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "unknownTaskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		List<KeyValuePair> fromWorker = worker.getMapResult("mrtUuid", "taskUuid");
		assertNotNull(fromWorker);
		assertTrue(fromWorker.isEmpty());
	}
	
	@Test
	public void shouldReturnEmpyListForUnknownMapReduceIdForReduceResult() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("hello", "1")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make another context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("anotherMrtUuid"));
			oneOf(task).getUUID(); will(returnValue("unknownTaskUuid"));
			oneOf(ctxFactory).createContext("anotherMrtUuid", "unknownTaskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		List<String> fromWorker = worker.getReduceResult("mrtUuid", "taskUuid");
		assertNotNull(fromWorker);
		assertTrue(fromWorker.isEmpty());
	}

	@Test
	public void shouldReturnEmpyListForKnownMapReduceIdButUnknownTaskUuidForReduceResult() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("hello", "1")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make another context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("unknownTaskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "unknownTaskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		List<String> fromWorker = worker.getReduceResult("mrtUuid", "taskUuid");
		assertNotNull(fromWorker);
		assertTrue(fromWorker.isEmpty());
	}
	
	@Test
	public void shouldReturnMapValuesFromContext() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("hello", "1")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			
			// returning values from context
			oneOf(ctx).getMapResult(); will(returnValue(result));
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result, worker.getMapResult("mrtUuid", "taskUuid"));
	}
	
	@Test
	public void shouldReturnReduceValuesFromContext() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<String> result = Arrays.asList(new String[]{"hello", "world"});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			
			// returning values from context
			oneOf(ctx).getReduceResult(); will(returnValue(result));
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result, worker.getReduceResult("mrtUuid", "taskUuid"));
	}
	
	@Test
	public void shouldHandleMultipleMapTaskUUIDs() {
		final Context ctx1 = this.mockery.mock(Context.class, "ctx1");
		final Context ctx2 = this.mockery.mock(Context.class, "ctx2");
		ExactCommandExecutor exec = new ExactCommandExecutor(2);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result1 = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1")});
		final List<KeyValuePair> result2 = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key2", "val2")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid1"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid1"); will(returnValue(ctx1));
			oneOf(task).runTask(ctx1);
			inSequence(events);
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid2"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid2"); will(returnValue(ctx2));
			oneOf(task).runTask(ctx2);
			inSequence(events);
			
			// returning values from context
			oneOf(ctx1).getReduceResult(); will(returnValue(result1));
			inSequence(events);
			oneOf(ctx2).getReduceResult(); will(returnValue(result2));
		}});
		worker.executeTask(task);
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result1, worker.getReduceResult("mrtUuid", "taskUuid1"));
		assertSame(result2, worker.getReduceResult("mrtUuid", "taskUuid2"));
	}

	@Test
	public void shouldHandleMultipleReduceTaskUUIDs() {
		final Context ctx1 = this.mockery.mock(Context.class, "ctx1");
		final Context ctx2 = this.mockery.mock(Context.class, "ctx2");
		ExactCommandExecutor exec = new ExactCommandExecutor(2);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<String> result1 = Arrays.asList(new String[]{"hello"});
		final List<String> result2 = Arrays.asList(new String[]{"world"});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid1"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid1"); will(returnValue(ctx1));
			oneOf(task).runTask(ctx1);
			inSequence(events);
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid2"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid2"); will(returnValue(ctx2));
			oneOf(task).runTask(ctx2);
			inSequence(events);
			
			// returning values from context
			oneOf(ctx1).getReduceResult(); will(returnValue(result1));
			inSequence(events);
			oneOf(ctx2).getReduceResult(); will(returnValue(result2));
		}});
		worker.executeTask(task);
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result1, worker.getReduceResult("mrtUuid", "taskUuid1"));
		assertSame(result2, worker.getReduceResult("mrtUuid", "taskUuid2"));
	}
	
	@Test
	public void shouldNotRemoveMapResultsWhenReading() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key", "val")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			inSequence(events);
			
			// returning values from context
			oneOf(ctx).getMapResult(); will(returnValue(result));
			oneOf(ctx).getMapResult(); will(returnValue(result));
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result, worker.getMapResult("mrtUuid", "taskUuid"));
		assertSame(result, worker.getMapResult("mrtUuid", "taskUuid"));
	}
	
	@Test
	public void shouldNotRemoveReduceResultsWhenReading() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<String> result = Arrays.asList(new String[]{"hello"});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			inSequence(events);
			
			// returning values from context
			oneOf(ctx).getReduceResult(); will(returnValue(result));
			oneOf(ctx).getReduceResult(); will(returnValue(result));
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result, worker.getReduceResult("mrtUuid", "taskUuid"));
		assertSame(result, worker.getReduceResult("mrtUuid", "taskUuid"));
	}
	
	@Test
	public void shouldCleanMapResultsInContext() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<KeyValuePair> result = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key", "val")});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			inSequence(events);
			
			// returning values from context
			oneOf(ctx).getMapResult(); will(returnValue(result));
			oneOf(ctx).destroy();
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result, worker.getMapResult("mrtUuid", "taskUuid"));
		worker.cleanSpecificResult("mrtUuid", "taskUuid");
		assertNotNull(worker.getMapResult("mrtUuid", "taskUuid"));
		assertTrue(worker.getMapResult("mrtUuid", "taskUuid").isEmpty());
	}
	
	@Test
	public void shouldCleanReduceResultsInContext() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		final List<String> result = Arrays.asList(new String[]{"hello"});
		this.mockery.checking(new Expectations() {{ 
			// running task in order to make context available
			oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrtUuid"));
			oneOf(task).getUUID(); will(returnValue("taskUuid"));
			oneOf(ctxFactory).createContext("mrtUuid", "taskUuid"); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			inSequence(events);
			
			// returning values from context
			oneOf(ctx).getReduceResult(); will(returnValue(result));
			oneOf(ctx).destroy();
		}});
		worker.executeTask(task);
		assertTrue(exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS));
		assertSame(result, worker.getReduceResult("mrtUuid", "taskUuid"));
		worker.cleanSpecificResult("mrtUuid", "taskUuid");
		assertNotNull(worker.getReduceResult("mrtUuid", "taskUuid"));
		assertTrue(worker.getReduceResult("mrtUuid", "taskUuid").isEmpty());
	}
	
	
	@Test
	public void shouldDoNothingWhenAskedToCleanInexistentMapReduceUUID() {
		ExecutorService exec = Executors.newSingleThreadExecutor();
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		worker.cleanAllResults("iDoNotExist");
	}
	
	@Test
	public void shouldDoNothingWhenAskedToCleanInexistentTaskUUID() {
		ExecutorService exec = Executors.newSingleThreadExecutor();
		Pool pool = new Pool(Executors.newSingleThreadExecutor());
		pool.init();
		ThreadWorker worker = new ThreadWorker(pool, exec, ctxFactory);
		worker.cleanSpecificResult("inexistentMapReduceTaskUuid", "inexistentTaskUuid");
	}
}