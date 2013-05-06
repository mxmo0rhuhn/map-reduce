package ch.zhaw.mapreduce;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.inject.Named;

import ch.zhaw.mapreduce.impl.InMemoryShuffler;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;
import ch.zhaw.mapreduce.plugins.Loader;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;

/**
 * In dieser Klasse befinden sich die Bindings für Guice - also welche Implementationen für welche Interfaces verwendet
 * werden sollen.
 * 
 * @author Reto
 * 
 */
public class MapReduceConfig extends AbstractModule {

	/**
	 * Binded verschiedene Interfaces zu den zugehörigen Implementationen.
	 */
	@Override
	protected void configure() {

		// AssistedInject Magic: Mit diesem FactoryModuleBuilder wird ein Binding für die RunnerFactory erstellt ohne,
		// dass wir eine tatsächliche Implementation bereitstellen.
		install(new FactoryModuleBuilder().implement(MapWorkerTask.class, MapWorkerTask.class)
				.implement(ReduceWorkerTask.class, ReduceWorkerTask.class).build(WorkerTaskFactory.class));

		bind(Master.class);
		bind(Pool.class);
		bind(Loader.class);
		bind(Shuffler.class).to(InMemoryShuffler.class);
		
		bind(String.class).annotatedWith(Names.named("plugins.property")).toInstance("mrplugins");
		
		// see PostConstructFeature
		bindListener(Matchers.any(), new PostConstructFeature());
	}

	@Provides
	@Named("poolExecutor")
	public Executor createPoolExec() {
		return Executors.newSingleThreadExecutor();
	}
	
	@Provides
	@Named("mapReduceTaskUuid")
	public String getMapReduceTaskUuid() {
		return UUID.randomUUID().toString();
	}
	
	@Provides
	@Named("taskUuid")
	public String getWorkerTaskUuid() {
		return UUID.randomUUID().toString();
	}
	
}