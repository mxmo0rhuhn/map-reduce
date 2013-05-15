package ch.zhaw.mapreduce;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Named;

import ch.zhaw.mapreduce.impl.FilePersistence;
import ch.zhaw.mapreduce.impl.InMemoryShuffler;
import ch.zhaw.mapreduce.plugins.Loader;
import ch.zhaw.mapreduce.plugins.socket.impl.NamedThreadFactory;

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
		install(new FactoryModuleBuilder().build(WorkerTaskFactory.class));
		install(new FactoryModuleBuilder().build(MasterFactory.class));

		bind(Pool.class);
		bind(Loader.class);
		bind(Shuffler.class).to(InMemoryShuffler.class);
		bind(Persistence.class).to(FilePersistence.class);
		bind(String.class).annotatedWith(Names.named("filepersistence.directory")).toInstance(System.getProperty("java.io.tmpdir") + "/socket/filepers/");
		bind(String.class).annotatedWith(Names.named("filepersistence.ending")).toInstance(".ser");
				
		bind(String.class).annotatedWith(Names.named("plugins.property")).toInstance("mrplugins");
		bind(Long.class).annotatedWith(Names.named("StatisticsPrinterTimeout")).toInstance(10000L);
		// 10s
		bind(Long.class).annotatedWith(Names.named("memoryFullSleepTime")).toInstance(10000L);
		
		// see PostConstructFeature
		bindListener(Matchers.any(), new PostConstructFeature());
	}

	@Provides
	@Named("poolExecutor")
	public Executor createPoolExec() {
		return Executors.newSingleThreadExecutor(new NamedThreadFactory("PoolExecutor"));
	}
	
	@Provides
	@Named("SupervisorScheduler")
	public ScheduledExecutorService poolSupervisor() {
		return Executors.newScheduledThreadPool(1, new NamedThreadFactory("PoolSupervisor"));
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
