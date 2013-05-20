package ch.zhaw.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.auto.Auto;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.thread.ThreadWorker;

public class PoolTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery(){{setThreadingPolicy(new Synchroniser());}};
	
	@Auto
	private Sequence events;
	
	@Mock
	private Worker worker;
	
	@Mock
	private WorkerTask task;
	
	@Mock
	private Context ctx;
	
	@Mock
	private ExecutorService execMock;
	
	@Mock
	private Provider<Context> ctxProvider;
	
	@Mock
	private Persistence persistence;
	
	private Executor executor = Executors.newSingleThreadExecutor();
	
	private ScheduledExecutorService sExec = Executors.newSingleThreadScheduledExecutor();

	@Test
	public void shouldHaveZeroInitialWorker() {
		Pool p = new Pool(executor, 1, 2, sExec, 1);
		p.init();
		assertEquals(0, p.getCurrentPoolSize());
		assertEquals(0, p.getFreeWorkers());
	}

	@Test
	public void shouldHaveOneWorker() {
		Pool p = new Pool(executor, 1, 2, sExec, 1);
		p.init();
		p.donateWorker(worker);
		assertEquals(1, p.getCurrentPoolSize());
		assertEquals(1, p.getFreeWorkers());
	}

	@Test
	public void shouldHaveTwoWorker() {
		Worker w1 = this.mockery.mock(Worker.class, "w1");
		Worker w2 = this.mockery.mock(Worker.class, "w2");
		Pool p = new Pool(executor, 1, 2, sExec, 1);
		p.init();
		p.donateWorker(w1);
		p.donateWorker(w2);
		assertEquals(2, p.getCurrentPoolSize());
		assertEquals(2, p.getFreeWorkers());
	}

	@Test(expected = IllegalStateException.class)
	public void shouldNotBeAbleToInitTwice() {
		Pool p = new Pool(executor, 1, 2, sExec, 1);
		try {
			p.init(); // first time must work
		} catch (IllegalStateException ise) {
			fail(ise.getMessage());
		}
		p.init();
	}

	@Test
	public void shouldExecuteWork() throws InterruptedException {
		final ExactCommandExecutor threadExec = new ExactCommandExecutor(1);
		Pool p = new Pool(executor, 1, 2, sExec, 1);
		p.init();
		final ThreadWorker worker = new ThreadWorker(p, threadExec, ctxProvider);
		p.donateWorker(worker);
		this.mockery.checking(new Expectations() {
			{
				oneOf(task).enqueued();
				inSequence(events);
				oneOf(task).getTaskUuid(); will(returnValue("taskUUID"));
				oneOf(ctxProvider).get(); will(returnValue(ctx));
				inSequence(events);
				oneOf(task).started();
				oneOf(task).runTask(ctx); will(throwException(new RuntimeException()));
				oneOf(task).fail();
			}
		});

		p.enqueueTask(task);
		assertTrue(threadExec.waitForExpectedTasks(300, TimeUnit.MILLISECONDS));
	}
	
	@Test
	public void shouldStopUponInterruption() throws InterruptedException {
		final Thread[] ts = new Thread[1];
		Executor exec = Executors.newSingleThreadExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable command) {
				ts[0] = new Thread(command);
				return ts[0];
			}
		});
		Pool p = new Pool(exec, 1, 2, sExec, 1);
		p.init();
		assertTrue(p.isRunning());
		Thread.sleep(200);
		ts[0].interrupt();
		Thread.yield();
		Thread.sleep(200);
		assertFalse(p.isRunning());
	}

}
