package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.impl.MapTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.ReduceTaskRunner;

import com.google.inject.Guice;

public class SocketClientConfigTest extends AbstractClientSocketMapReduceTest {
	
	
	@Test
	public void shouldCreateMapTaskRunner() {
		MapTaskRunnerFactory fac = Guice.createInjector(new SocketClientConfig(resCollector)).getInstance(MapTaskRunnerFactory.class);
		MapTaskRunner run = fac.createMapTaskRunner(mrtUuid, taskUuid, mapInstr, combInstr, mapInput);
		assertNotNull(run);
	}
	
	@Test
	public void shouldCreateReduceTaskRunner() {
		ReduceTaskRunnerFactory fac = Guice.createInjector(new SocketClientConfig(resCollector)).getInstance(ReduceTaskRunnerFactory.class);
		ReduceTaskRunner run = fac.createReduceTaskRunner(mrtUuid, taskUuid, redInstr, reduceKey, reduceValues);
		assertNotNull(run);
	}
	
	@Test
	public void shouldCreateSocketAgent() {
		SocketAgentFactory fac = Guice.createInjector(new SocketClientConfig(resCollector)).getInstance(SocketAgentFactory.class);
		SocketAgent sa = fac.createSocketAgent(clientIp);
		assertNotNull(sa);
	}
	
	@Test
	public void shouldCreateSuccessResult() {
		SocketAgentResultFactory fac = Guice.createInjector(new SocketClientConfig(resCollector)).getInstance(SocketAgentResultFactory.class);
		SocketAgentResult sar = fac.createFromTaskResult(taskResult);
		assertNotNull(sar);
	}
	
	@Test
	public void shouldCreateFailureResult() {
		SocketAgentResultFactory fac = Guice.createInjector(new SocketClientConfig(resCollector)).getInstance(SocketAgentResultFactory.class);
		SocketAgentResult sar = fac.createFromException(new Exception());
		assertNotNull(sar);
	}
	

}
