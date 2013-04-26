package ch.zhaw.mapreduce.plugins.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.junit.Test;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.registry.MapReduceConfig;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class ThreadConfigTest {

	@Test
	public void shouldCreateContextWithSpecifiedParametersInjected() {
		Injector injector = Guice.createInjector(new MapReduceConfig(), new ThreadConfig());
		Context ctx = injector.getInstance(ContextFactory.class).createContext("mapReduceUUID", "taskUUID");
		assertEquals("mapReduceUUID", ctx.getMapReduceTaskUUID());
		assertEquals("taskUUID", ctx.getTaskUUID());
	}
	
	@Test
	public void workerShouldBePrototype() {
		Injector inj = Guice.createInjector(new ThreadConfig());
		ThreadWorker w1 = (ThreadWorker) inj.getInstance(Worker.class);
		ThreadWorker w2 = (ThreadWorker) inj.getInstance(Worker.class);
		assertNotSame(w1, w2);
	}


}
