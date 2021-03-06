package ch.zhaw.mapreduce.plugins.thread;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.auto.Auto;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.PoolImpl;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

public class ThreadWorkerTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();

	@Auto
	private Sequence events;

	@Mock
	private WorkerTask task;

	@Mock
	private Provider<Context> ctxProvider;

	@Mock
	private Context ctx;

	@Mock
	private Persistence pers;

	@Mock
	private ExecutorService execMock;
	
	@Mock
	private MapInstruction mapInstr;
	
	@Mock
	private CombinerInstruction combInstr;
	
	@Mock
	private ReduceInstruction redInstr;

	@Test
	public void shouldGoBackToPool() throws InterruptedException {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		PoolImpl p = new PoolImpl(Executors.newSingleThreadExecutor());
		p.init();
		final ThreadWorker worker = new ThreadWorker(p, exec, ctxProvider);
		p.donateWorker(worker);
		final MapWorkerTask task = new MapWorkerTask("taskUuid", pers, mapInstr, null, "input");
		this.mockery.checking(new Expectations() {
			{
				allowing(mapInstr); 
				oneOf(ctxProvider).get(); will(returnValue(ctx));
				oneOf(ctx).getMapResult(); will(returnValue(new ArrayList<KeyValuePair>()));
			}
		});
		p.enqueueTask(task);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
		assertEquals(1, p.getFreeWorkers());
	}

	@Test
	public void shouldPersistReduceResultOnly() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		PoolImpl p = new PoolImpl(Executors.newSingleThreadExecutor());
		p.init();
		final ThreadWorker worker = new ThreadWorker(p, exec, ctxProvider);
		final ReduceWorkerTask task = new ReduceWorkerTask("taskUuid", pers, redInstr, "key", new ArrayList<KeyValuePair>());
		this.mockery.checking(new Expectations() {
			{
				allowing(redInstr);
				oneOf(ctxProvider).get(); will(returnValue(ctx));
				oneOf(ctx).getReduceResult(); will(returnValue(new ArrayList<String>()));
			}
		});
		worker.executeTask(task);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
	}

	@Test
	public void shouldSetTaskToFailedOnException() throws Exception  {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		PoolImpl p = new PoolImpl(Executors.newSingleThreadExecutor());
		p.init();
		final ThreadWorker worker = new ThreadWorker(p, exec, ctxProvider);
		this.mockery.checking(new Expectations() {
			{
				oneOf(task).getTaskUuid(); will(returnValue("taskUuid"));
				oneOf(task).started();
				oneOf(ctxProvider).get(); will(returnValue(ctx));
				oneOf(task).runTask(ctx); will(throwException(new RuntimeException()));
				oneOf(task).fail();
			}
		});
		worker.executeTask(task);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
	}


}