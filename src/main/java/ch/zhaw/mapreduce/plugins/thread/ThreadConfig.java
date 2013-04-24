package ch.zhaw.mapreduce.plugins.thread;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import ch.zhaw.mapreduce.Worker;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class ThreadConfig extends AbstractModule {
	
	public static int NWORKERS = 500;

	@Override
	protected void configure() {
		bind(Worker.class).to(ThreadWorker.class);
		bind(Integer.class).annotatedWith(Names.named("thread.nrworkers")).toInstance(NWORKERS);
		bind(Executor.class).toInstance(Executors.newSingleThreadExecutor());
	}

}
