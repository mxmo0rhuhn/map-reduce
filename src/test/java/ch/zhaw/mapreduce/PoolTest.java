package ch.zhaw.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.zhaw.mapreduce.plugins.thread.ThreadWorker;

@RunWith(JMock.class)
public class PoolTest {

	private Mockery context;

	private Executor executor;

	@Before
	public void initMock() {
		this.context = new JUnit4Mockery();
		this.executor = Executors.newSingleThreadExecutor();
	}

	@Test
	public void shouldHaveZeroInitialWorker() {
		Pool p = new Pool(executor);
		p.init();
		assertEquals(0, p.getCurrentPoolSize());
		assertEquals(0, p.getFreeWorkers());
	}

	@Test
	public void shouldHaveOneWorker() {
		Worker w = this.context.mock(Worker.class);
		Pool p = new Pool(executor);
		p.init();
		p.donateWorker(w);
		assertEquals(1, p.getCurrentPoolSize());
		assertEquals(1, p.getFreeWorkers());
	}

	@Test
	public void shouldHaveTwoWorker() {
		Worker w1 = this.context.mock(Worker.class, "w1");
		Worker w2 = this.context.mock(Worker.class, "w2");
		Pool p = new Pool(executor);
		p.init();
		p.donateWorker(w1);
		p.donateWorker(w2);
		assertEquals(2, p.getCurrentPoolSize());
		assertEquals(2, p.getFreeWorkers());
	}

	@Test(expected = IllegalStateException.class)
	public void shouldNotBeAbleToInitTwice() {
		Pool p = new Pool(this.executor);
		try {
			p.init(); // first time must work
		} catch (IllegalStateException ise) {
			fail(ise.getMessage());
		}
		p.init();
	}

	@Test
	public void shouldExecuteWork() throws InterruptedException {
		final WorkerTask task = this.context.mock(WorkerTask.class);
		final ContextFactory ctxFactory = this.context.mock(ContextFactory.class);
		final Context ctx = this.context.mock(Context.class);
		final Executor poolExec = Executors.newSingleThreadExecutor();
		final ExactCommandExecutor threadExec = new ExactCommandExecutor(1);
		Pool p = new Pool(poolExec);
		p.init();
		final ThreadWorker worker = new ThreadWorker(p, threadExec, ctxFactory);
		p.donateWorker(worker);
		this.context.checking(new Expectations() {
			{
				oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrTaskUUID"));
				oneOf(task).getUUID(); will(returnValue("taskUUID"));
				oneOf(ctxFactory).createContext("mrTaskUUID", "taskUUID"); will(returnValue(ctx));
				oneOf(task).setWorker(worker);
				oneOf(task).runTask(ctx);
			}
		});

		p.enqueueWork(task);
		assertTrue(threadExec.waitForExpectedTasks(300, TimeUnit.MILLISECONDS));
	}

	@Test
	public void workerShouldBeFreeAgainAfterwards() {
		final WorkerTask task = this.context.mock(WorkerTask.class);
		final ContextFactory ctxFactory = this.context.mock(ContextFactory.class);
		final Context ctx = this.context.mock(Context.class);
		final Executor poolExec = Executors.newSingleThreadExecutor();
		final ExactCommandExecutor threadExec = new ExactCommandExecutor(1);
		Pool p = new Pool(poolExec);
		p.init();
		final ThreadWorker worker = new ThreadWorker(p, threadExec, ctxFactory);
		p.donateWorker(worker);
		this.context.checking(new Expectations() {
			{
				oneOf(task).getMapReduceTaskUUID(); will(returnValue("mrTaskUUID"));
				oneOf(task).getUUID(); will(returnValue("taskUUID"));
				oneOf(ctxFactory).createContext("mrTaskUUID", "taskUUID"); will(returnValue(ctx));
				oneOf(task).setWorker(worker);
				oneOf(task).runTask(ctx);
			}
		});

		assertEquals(1, p.getFreeWorkers());
		p.enqueueWork(task);
		assertTrue(threadExec.waitForExpectedTasks(300, TimeUnit.MILLISECONDS));
		assertEquals(1, p.getFreeWorkers());
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
		Pool pool = new Pool(exec);
		pool.init();
		assertTrue(pool.isRunning());
		Thread.sleep(200);
		ts[0].interrupt();
		Thread.yield();
		Thread.sleep(200);
		assertFalse(pool.isRunning());
	}

}
