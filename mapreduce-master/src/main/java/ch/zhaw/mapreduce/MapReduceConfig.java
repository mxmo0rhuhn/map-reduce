package ch.zhaw.mapreduce;

import static ch.zhaw.mapreduce.MapReduceUtil.loadWithOptionalDefaults;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.inject.Named;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.impl.FilePersistence;
import ch.zhaw.mapreduce.impl.InMemoryShuffler;
import ch.zhaw.mapreduce.impl.PoolImpl;
import ch.zhaw.mapreduce.impl.PoolStatisticsPrinter;
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
		try {
			Names.bindProperties(binder(),
					loadWithOptionalDefaults("mapreduce-defaults.properties", "mapreduce.properties"));
		} catch (IOException e) {
			addError(e);
		}

		bind(Master.class);
		bind(Pool.class).to(PoolImpl.class).in(Singleton.class);
		bind(Loader.class);
		bind(Shuffler.class).to(InMemoryShuffler.class);
		bind(Persistence.class).to(FilePersistence.class);

		bind(PoolStatisticsPrinter.class).asEagerSingleton();

		install(new FactoryModuleBuilder().build(WorkerTaskFactory.class));

		// see PostConstructFeature
		bindListener(Matchers.any(), new PostConstructFeature());
	}

	@Provides
	@Named("taskUuid")
	private String genTaskUuid() {
		return UUID.randomUUID().toString();
	}

	@Provides
	@Named("poolExecutor")
	private Executor createPoolExec() {
		return Executors.newSingleThreadExecutor(new NamedThreadFactory("PoolExecutor"));
	}

	@Provides
	@Named("supervisorScheduler")
	private ScheduledExecutorService poolSupervisor() {
		return Executors.newScheduledThreadPool(1, new NamedThreadFactory("PoolSupervisor"));
	}
}
