package ch.zhaw.mapreduce.plugins.socket;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentRegistratorImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentTaskFactoryImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.NamedThreadFactory;
import ch.zhaw.mapreduce.plugins.socket.impl.ResultCleanerTask;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketResultCollectorImpl;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import de.root1.simon.Registry;
import de.root1.simon.Simon;

public class SocketServerConfig extends AbstractModule {
	
	private static final Logger LOG = Logger.getLogger(SocketServerConfig.class.getName());
	
	@Override
	protected void configure() {
		install(new SharedSocketConfig());
		
		// Simon Registry Bindings
		bind(AgentRegistrator.class).to(AgentRegistratorImpl.class);
		bind(SocketResultCollector.class).to(SocketResultCollectorImpl.class).in(Singleton.class);
		bind(ResultCleanerTask.class).asEagerSingleton();
		bind(AgentTaskFactory.class).to(AgentTaskFactoryImpl.class);
		
		install(new FactoryModuleBuilder().implement(Worker.class, SocketWorker.class).build(SocketWorkerFactory.class));

		try {
			bind(Registry.class).toInstance(Simon.createRegistry(4753));
		} catch (IOException e) {
			addError(e);
		}
	}
	
	@Provides
	@Singleton
	@Named("SocketScheduler")
	private ScheduledExecutorService socketScheduler(@Named("SocketSchedulerPoolSize") int poolSize) {
		// fuer allerlei scheduling tasks
		return Executors.newScheduledThreadPool(poolSize, new NamedThreadFactory("SocketScheduler"));
	}
	
	@Provides
	@Singleton
	@Named("SocketWorkerTaskTriggerService")
	private ExecutorService taskrunnerservice() {
		// um vom socketworker den rpc zum socketagent zu machen (tasks ausführen)
		LOG.info("Using CachedThreadPool for SocketWorkerTaskTriggerService");
		return Executors.newCachedThreadPool(new NamedThreadFactory("SocketWorkerTaskTriggerService"));
	}
	
}
