package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.concurrent.ExecutorService;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.impl.MapTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.ReduceTaskRunner;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class SocketClientConfigTest extends AbstractClientSocketMapReduceTest {

	@Test
	public void shouldCreateMapTaskRunner() {
		MapTaskRunnerFactory fac = Guice.createInjector(new SocketClientConfig(resCollector, 1)).getInstance(
				MapTaskRunnerFactory.class);
		MapTaskRunner run = fac.createMapTaskRunner(taskUuid, mapInstr, combInstr, mapInput);
		assertNotNull(run);
	}

	@Test
	public void shouldCreateReduceTaskRunner() {
		ReduceTaskRunnerFactory fac = Guice.createInjector(new SocketClientConfig(resCollector, 1)).getInstance(
				ReduceTaskRunnerFactory.class);
		ReduceTaskRunner run = fac.createReduceTaskRunner(taskUuid, redInstr, reduceKey, reduceValues);
		assertNotNull(run);
	}

	@Test
	public void shouldCreateSocketAgent() {
		SocketAgentFactory fac = Guice.createInjector(new SocketClientConfig(resCollector, 1)).getInstance(
				SocketAgentFactory.class);
		SocketAgent sa = fac.createSocketAgent(clientIp);
		assertNotNull(sa);
	}

	@Test
	public void shouldCreateSuccessResult() {
		SocketAgentResultFactory fac = Guice.createInjector(new SocketClientConfig(resCollector, 1)).getInstance(
				SocketAgentResultFactory.class);
		mockery.checking(new Expectations() {
			{
				oneOf(taskResult).wasSuccessful();
				will(returnValue(true));
				oneOf(taskResult).getResult();
				will(returnValue(mapResult));
			}
		});
		SocketAgentResult sar = fac.createFromTaskResult(taskUuid, taskResult);
		assertNotNull(sar);
		assertNull(sar.getException());
		assertEquals(taskUuid, sar.getTaskUuid());
		assertEquals(mapResult, sar.getResult());
	}

	@Test
	public void shouldCreateFailureResult() {
		SocketAgentResultFactory fac = Guice.createInjector(new SocketClientConfig(resCollector, 1)).getInstance(
				SocketAgentResultFactory.class);
		final Exception e = new Exception();
		mockery.checking(new Expectations() {
			{
				oneOf(taskResult).wasSuccessful();
				will(returnValue(false));
				oneOf(taskResult).getException();
				will(returnValue(e));
			}
		});
		SocketAgentResult sar = fac.createFromTaskResult(taskUuid, taskResult);
		assertNotNull(sar);
		assertNull(sar.getResult());
		assertEquals(taskUuid, sar.getTaskUuid());
		assertEquals(e, sar.getException());
	}

	@Test
	public void shouldCreateFailure() {
		SocketAgentResultFactory fac = Guice.createInjector(new SocketClientConfig(resCollector, 1)).getInstance(
				SocketAgentResultFactory.class);
		Exception e = new Exception();
		SocketAgentResult sar = fac.createFromException(taskUuid, e);
		assertNotNull(sar);
		assertNull(sar.getResult());
		assertEquals(taskUuid, sar.getTaskUuid());
		assertEquals(e, sar.getException());
	}

	@Test
	public void pusherServiceShouldBeSingleton() {
		Injector injector = Guice.createInjector(new SocketClientConfig(resCollector, 1));
		ExecutorService exec1 = injector.getInstance(Key.get(ExecutorService.class, Names.named("resultPusherService")));
		ExecutorService exec2 = injector.getInstance(Key.get(ExecutorService.class, Names.named("resultPusherService")));
		assertSame(exec1, exec2);
	}
	
	@Test
	public void runnerServiceShouldBeSingleton() {
		Injector injector = Guice.createInjector(new SocketClientConfig(resCollector, 1));
		ExecutorService exec1 = injector.getInstance(Key.get(ExecutorService.class, Names.named("taskRunnerService")));
		ExecutorService exec2 = injector.getInstance(Key.get(ExecutorService.class, Names.named("taskRunnerService")));
		assertSame(exec1, exec2);
	}
}
