package ch.zhaw.mapreduce.plugins.thread;

import static org.junit.Assert.assertNotSame;

import javax.inject.Provider;

import org.junit.Test;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.Worker;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class ThreadConfigTest {

	@Test
	public void ctxShouldBePrototype() {
		Injector injector = Guice.createInjector(new MapReduceConfig(), new ThreadConfig());
		Provider<Context> prov = injector.getProvider(Context.class);
		assertNotSame(prov.get(), prov.get());
	}
	
	@Test
	public void workerShouldBePrototype() {
		Injector inj = Guice.createInjector(new MapReduceConfig(), new ThreadConfig());
		ThreadWorker w1 = (ThreadWorker) inj.getInstance(Worker.class);
		ThreadWorker w2 = (ThreadWorker) inj.getInstance(Worker.class);
		assertNotSame(w1, w2);
	}


}
