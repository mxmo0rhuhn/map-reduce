package ch.zhaw.mapreduce.plugins.thread;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.impl.ContextImpl;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class ThreadConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(Worker.class).to(ThreadWorker.class);
		bind(ExecutorService.class).annotatedWith(Names.named("ThreadWorker")).toInstance(
				Executors.newSingleThreadExecutor());
		bind(Context.class).to(ContextImpl.class);

		int nWorkers;
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("mapreduce.properties"));

			nWorkers = Integer.parseInt(prop.getProperty("nThreadWorkers"));

		} catch (Exception e) {
			// konnten nicht geladen werden - weiter mit oben definierten defaults
			nWorkers = Runtime.getRuntime().availableProcessors() + 1;
		}

		bind(Integer.class).annotatedWith(Names.named("nWorkers")).toInstance(nWorkers);
	}
}
