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

		// 10000 Milisekunden => 10 Sekunden
		
		// RAM beschränkung
		// Intervall, in dem Statistiken geladen werden
		long StatisticsPrinterTimeout = 10000L;
		// Prozentzahl an RAMusage, ab der keine Tasks mehr angenommen werden
		long minRemainingMemory = 10L;
		// Zeit, die die Applikation keine neuen Tasks mehr annimt, wenn der RAM "voll" ist
		long memoryFullSleepTime = 10000L;

		// Plugins, die durch den Loader geladen werden sollen
		String plugins = "Thread";
		
		
		// Rescheduling
		// Prozentzahl an bereits erfüllten Aufgaben, bei der das Rescheduling von Tasks startet
		long rescheduleStartPercentage = 90;
		// Anzahl an Abfragen auf die Fertigstellung der Tasks, die vergehen muss, bis wieder Tasks rescheduled werden
		long rescheduleEvery  = 10;
		// Zeit in Millisekunden die gewartet wird, zwischen den einzelnen Abfragen ob Aufgaben bereits erledigt sind
		long waitTime = 1000;

		Properties prop = new Properties();
		try {
    		prop.load(new FileInputStream("mapReduce.properties"));
			
			StatisticsPrinterTimeout = Long.parseLong(prop.getProperty("statisticsPrinterTimeout"));
			memoryFullSleepTime = Long.parseLong(prop.getProperty("memoryFullSleepTime"));
			minRemainingMemory = Long.parseLong(prop.getProperty("minRemainingMemory"));
			
			plugins = prop.getProperty("plugins");
			
			rescheduleStartPercentage = Long.parseLong(prop.getProperty("rescheduleStartPercentage"));
			rescheduleEvery = Long.parseLong(prop.getProperty("rescheduleEvery"));
			waitTime = Long.parseLong(prop.getProperty("waitTime"));
			
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
		
		bind(Long.class).annotatedWith(Names.named("rescheduleStartPercentage")).toInstance(
				rescheduleStartPercentage);
		bind(Long.class).annotatedWith(Names.named("rescheduleEvery")).toInstance(
				rescheduleEvery);
		bind(Long.class).annotatedWith(Names.named("waitTime")).toInstance(
				waitTime);

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
	@Named("taskUuid")
	public String getWorkerTaskUuid() {
		// TODO das wird im maste rnicht mehr verwendent! wieder einfuehren. evt via Provider<String>
		return UUID.randomUUID().toString();
	}

}
