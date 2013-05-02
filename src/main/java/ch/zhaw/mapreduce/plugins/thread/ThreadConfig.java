package ch.zhaw.mapreduce.plugins.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.impl.FilePersistence;
import ch.zhaw.mapreduce.impl.LocalContext;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public class ThreadConfig extends AbstractModule {
	
	public static int NWORKERS = 5;

	@Override
	protected void configure() {
		bind(Worker.class).to(ThreadWorker.class);
		
		bind(Integer.class).annotatedWith(Names.named("thread.nrworkers")).toInstance(NWORKERS);
		bind(ExecutorService.class).annotatedWith(Names.named("ThreadWorker")).toInstance(Executors.newSingleThreadExecutor());
		bind(Persistence.class).to(FilePersistence.class);
		
		bind(String.class).annotatedWith(Names.named("filepersistence.directory")).toInstance(System.getProperty("java.io.tmpdir") + "/mapred/filepers/");
		bind(String.class).annotatedWith(Names.named("filepersistence.ending")).toInstance(".ser");
		
		install(new FactoryModuleBuilder().implement(Context.class, LocalContext.class).build(ContextFactory.class));
	}

}
