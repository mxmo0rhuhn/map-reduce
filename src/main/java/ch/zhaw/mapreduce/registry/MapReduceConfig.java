package ch.zhaw.mapreduce.registry;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import ch.zhaw.mapreduce.Master;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Shuffler;
import ch.zhaw.mapreduce.WorkerTaskFactory;
import ch.zhaw.mapreduce.impl.InMemoryShuffler;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;
import ch.zhaw.mapreduce.workers.ThreadWorker;
import ch.zhaw.mapreduce.workers.Worker;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.matcher.Matchers;

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

		bind(Worker.class).to(ThreadWorker.class);
		bind(Pool.class);
		
		// Master soll einfach von Guice verwaltet werden. Ohne Interface
		bind(Master.class);
		bind(Shuffler.class).to(InMemoryShuffler.class);

		// see PostConstructFeature
		bindListener(Matchers.any(), new PostConstructFeature());
	}

	/**
	 * Provided eine Implementation für das Interface ExecutorService, welches mit der Annotation SingleThreaded
	 * annotiert ist.
	 * 
	 */
	@Provides
	@WorkerExecutor
	public Executor createWorkerExec() {
		return Executors.newSingleThreadExecutor();
	}

	@Provides
	@PoolExecutor
	public Executor createPoolExec() {
		return Executors.newSingleThreadExecutor();
	}
	
	@Provides
	@MapReduceTaskUUID
	public String getMapReduceTaskUUID() {
		return UUID.randomUUID().toString();
	}
	
}
