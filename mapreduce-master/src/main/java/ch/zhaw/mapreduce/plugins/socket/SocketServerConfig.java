package ch.zhaw.mapreduce.plugins.socket;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.impl.FilePersistence;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentRegistratorImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentTaskFactoryImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.NamedThreadFactory;
import ch.zhaw.mapreduce.plugins.socket.impl.ResultCleanerTask;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketResultCollectorImpl;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

import de.root1.simon.Registry;
import de.root1.simon.Simon;

public class SocketServerConfig extends AbstractModule {
	
	private static final Logger LOG = Logger.getLogger(SocketServerConfig.class.getName());
	
	private static final long SECOND = 1000;
	
	private static final long MINUTE = 60 * SECOND;
	
	@Override
	protected void configure() {
		install(new SharedSocketConfig());
		
		// Simon Registry Bindings
		bind(AgentRegistrator.class).to(AgentRegistratorImpl.class);
		bind(SocketResultCollector.class).to(SocketResultCollectorImpl.class).in(Singleton.class);
		
		// ResultCleaner
		bind(ResultCleanerTask.class).asEagerSingleton();
		bind(Long.class).annotatedWith(Names.named("AvailableResultTimeToLive")).toInstance(longProp("AvailableResultTimeToLive", 1*MINUTE));
		bind(Long.class).annotatedWith(Names.named("RequestedResultTimeToLive")).toInstance(longProp("RequestedResultTimeToLive", 10*MINUTE));
		bind(Long.class).annotatedWith(Names.named("SocketResultCleanupSchedulingDelay")).toInstance(longProp("SocketResultCleanupSchedulingDelay", MINUTE));
		
		// SocketWorker
		bind(AgentTaskFactory.class).to(AgentTaskFactoryImpl.class);
		bind(Persistence.class).to(FilePersistence.class);
		bind(Integer.class).annotatedWith(Names.named("ObjectByteCacheSize")).toInstance(intProp("ObjectByteCacheSize", 30));
		bind(Long.class).annotatedWith(Names.named("AgentTaskTriggeringTimeout")).toInstance(longProp("AgentTaskTriggeringTimeout", 2*SECOND));
		bind(Long.class).annotatedWith(Names.named("AgentPingerDelay")).toInstance(longProp("AgentPingerDelay",5*SECOND));
		install(new FactoryModuleBuilder().implement(Worker.class, SocketWorker.class).build(SocketWorkerFactory.class));

		bind(String.class).annotatedWith(Names.named("filepersistence.directory")).toInstance(System.getProperty("java.io.tmpdir") + "/socket/filepers/");
		bind(String.class).annotatedWith(Names.named("filepersistence.ending")).toInstance(".ser");
		
		try {
			bind(Registry.class).toInstance(Simon.createRegistry(intProp("simonport", 4753)));
		} catch (IOException e) {
			addError(e);
		}
	}
	
	@Provides
	@Singleton
	@Named("SocketScheduler")
	private ScheduledExecutorService socketScheduler() {
		// fuer allerlei scheduling tasks
		return Executors.newScheduledThreadPool(intProp("SocketSchedulerPoolSize", 5), new NamedThreadFactory("SocketScheduler"));
	}
	
	@Provides
	@Singleton
	@Named("SocketWorkerTaskTriggerService")
	private ExecutorService taskrunnerservice() {
		// um vom socketworker den rpc zum socketagent zu machen (tasks ausführen)
		LOG.info("Using CachedThreadPool for SocketWorkerTaskTriggerService");
		return Executors.newCachedThreadPool(new NamedThreadFactory("SocketWorkerTaskTriggerService"));
	}
	
	
	int intProp(String name) {return (int) longProp(name); };
	
	int intProp(String name, int def) { return (int) longProp(name, (long) def); }
	
	long longProp(String name) { return longProp(name, null); }
	
	long longProp(String name, Long def) {
		String propVal = System.getProperty(name);
		long retVal;
		if (propVal != null) {
			retVal = Long.parseLong(propVal);
		} else if (def == null)	{
			addError("System Property " + name + " is required");
			return -1;
		} else {
			retVal = def; // autoboxing
		}
		LOG.log(Level.CONFIG, "{0}={1}", new Object[] { name, retVal });
		return retVal;
	}

}
