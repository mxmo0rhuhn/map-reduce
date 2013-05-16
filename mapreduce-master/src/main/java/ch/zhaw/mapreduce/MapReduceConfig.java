package ch.zhaw.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
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
 * In dieser Klasse befinden sich die Bindings für Guice - also welche Implementationen für welche
 * Interfaces verwendet werden sollen.
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

		// 10000L Milisekunden => 10 Sekunden
		
		// Intervall, in dem Statistiken geladen werden
		long StatisticsPrinterTimeout = 10000L;

		// Prozentzahl an RAMusage, ab der keine Tasks mehr angenommen werden
		long minRemainingMemory = 10L;
		// Zeit, die die Applikation keine neuen Tasks mehr annimt, wenn der RAM "voll" ist
		long memoryFullSleepTime = 10000L;

		// Plugins, die durch den Loader geladen werden sollen
		String plugins = "Thread";

		Properties prop = new Properties();
		try {
    		prop.load(new FileInputStream("mapReduce.properties"));
			
			StatisticsPrinterTimeout = Long.parseLong(prop.getProperty("statisticsPrinterTimeout"));
			memoryFullSleepTime = Long.parseLong(prop.getProperty("memoryFullSleepTime"));
			minRemainingMemory = Long.parseLong(prop.getProperty("minRemainingMemory"));
			plugins = prop.getProperty("plugins");
			
		} catch (IOException e) {
			// konnten nicht geladen werden - weiter mit oben definierten defaults
			e.printStackTrace();
		}

		// AssistedInject Magic: Mit diesem FactoryModuleBuilder wird ein Binding für die
		// RunnerFactory erstellt ohne,
		// dass wir eine tatsächliche Implementation bereitstellen.
		install(new FactoryModuleBuilder().build(WorkerTaskFactory.class));
		install(new FactoryModuleBuilder().build(MasterFactory.class));
		bind(Pool.class);
		bind(Loader.class);
		bind(Shuffler.class).to(InMemoryShuffler.class);
		bind(Persistence.class).to(FilePersistence.class);

		// für die FilePersistence
		bind(String.class).annotatedWith(Names.named("filepersistence.directory")).toInstance(
				System.getProperty("java.io.tmpdir") + "/socket/filepers/");
		bind(String.class).annotatedWith(Names.named("filepersistence.ending")).toInstance(".ser");

		// Zeug das aus dem properties file entnommen wird
		bind(String.class).annotatedWith(Names.named("plugins")).toInstance(plugins);
		bind(Long.class).annotatedWith(Names.named("statisticsPrinterTimeout")).toInstance(
				StatisticsPrinterTimeout);
		bind(Long.class).annotatedWith(Names.named("memoryFullSleepTime")).toInstance(
				memoryFullSleepTime);
		bind(Long.class).annotatedWith(Names.named("minRemainingMemory")).toInstance(
				minRemainingMemory);

		// see PostConstructFeature
		bindListener(Matchers.any(), new PostConstructFeature());
	}

	@Provides
	@Named("poolExecutor")
	public Executor createPoolExec() {
		return Executors.newSingleThreadExecutor(new NamedThreadFactory("PoolExecutor"));
	}

	@Provides
	@Named("supervisorScheduler")
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
