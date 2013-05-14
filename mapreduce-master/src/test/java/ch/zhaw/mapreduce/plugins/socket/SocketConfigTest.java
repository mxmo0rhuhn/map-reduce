package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.Worker;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class SocketConfigTest extends AbstractMapReduceMasterSocketTest {
	
	private static final Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());

	@Test
	public void shouldCreateSocketTask() {
		mockery.checking(new Expectations() {{ oneOf(sAgent).getIp(); }});
		Worker worker = injector.getInstance(SocketWorkerFactory.class).createSocketWorker(sAgent, resCollector);
		assertNotNull(worker);
		assertTrue(worker instanceof SocketWorker);
	}

	@Test
	public void resultCollectorShouldBeSingleton() {
		SocketResultCollector col1 = injector.getInstance(SocketResultCollector.class);
		SocketResultCollector col2 = injector.getInstance(SocketResultCollector.class);
		assertSame(col1, col2);
	}

}
