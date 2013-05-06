package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.*;

import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.registry.MapReduceConfig;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class SocketConfigTest {
	
	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private SocketAgent agent;
	
	@Test
	public void shouldCreateSocketTask() {
		Injector injector = Guice.createInjector(new SocketServerConfig(), new MapReduceConfig());
		Worker worker = injector.getInstance(SocketWorkerFactory.class).createSocketWorker(agent);
		assertNotNull(worker);
		assertTrue(worker instanceof SocketWorker);
		SocketWorker sworker = (SocketWorker)worker;
		assertSame(agent, sworker.getSocketAgent());
	}

}
