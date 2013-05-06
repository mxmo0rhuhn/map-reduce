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
	private ClientCallback callback;
	
	@Test
	public void shouldCreateSocketTask() {
		Injector injector = Guice.createInjector(new SocketConfig(), new MapReduceConfig());
		Worker worker = injector.getInstance(SocketWorkerFactory.class).createSocketWorker("123.234.123.234", 7567, callback);
		assertNotNull(worker);
		assertTrue(worker instanceof SocketWorker);
		SocketWorker sworker = (SocketWorker)worker;
		assertEquals("123.234.123.234", sworker.getIp());
		assertEquals(7567, sworker.getPort());
		assertSame(callback, sworker.getCallback());
	}

}
