package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.*;

import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.Worker;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class SocketConfigTest {
	
	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private SocketAgent agent;
	
	@Mock
	private SocketResultCollector resCollector;
	
	@Test
	public void shouldCreateSocketTask() {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
		Worker worker = injector.getInstance(SocketWorkerFactory.class).createSocketWorker(agent, resCollector);
		assertNotNull(worker);
		assertTrue(worker instanceof SocketWorker);
	}
	
	@Test
	public void resultCollectorShouldBeSingleton() {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
		SocketResultCollector col1 = injector.getInstance(SocketResultCollector.class);
		SocketResultCollector col2 = injector.getInstance(SocketResultCollector.class);
		assertSame(col1, col2);
	}

}
