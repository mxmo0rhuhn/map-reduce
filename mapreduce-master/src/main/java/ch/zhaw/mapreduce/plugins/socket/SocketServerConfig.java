package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.impl.FilePersistence;
import ch.zhaw.mapreduce.impl.SocketResultCollectorImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentRegistratorImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentTaskFactoryImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.NamedThreadFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public class SocketServerConfig extends AbstractModule {
	
	private static final Logger LOG = Logger.getLogger(SocketServerConfig.class.getName());
	
	private static final int DEFAULT_TASKRUNNER_SERVICE_POOL_SIZE = 1;
	
	@Override
	protected void configure() {
		install(new SharedSocketConfig());
		
		bind(AgentRegistrator.class).to(AgentRegistratorImpl.class);
		bind(AgentTaskFactory.class).to(AgentTaskFactoryImpl.class);
		bind(Persistence.class).to(FilePersistence.class);
		bind(SocketResultCollector.class).to(SocketResultCollectorImpl.class).in(Singleton.class);
		bind(Integer.class).annotatedWith(Names.named("socket.masterpoolsize")).toInstance(1); 
		bind(Long.class).annotatedWith(Names.named("agentTaskTriggeringTimeout")).toInstance((long)2000);
		
		install(new FactoryModuleBuilder().implement(Worker.class, SocketWorker.class).build(SocketWorkerFactory.class));

		bind(String.class).annotatedWith(Names.named("filepersistence.directory")).toInstance(System.getProperty("java.io.tmpdir") + "/socket/filepers/");
		bind(String.class).annotatedWith(Names.named("filepersistence.ending")).toInstance(".ser");
	}
	
	@Provides
	@Singleton
	@Named("taskrunnerservice")
	private ExecutorService taskrunnerservice() {
		int poolSize = sysProp("socket.taskrunnerservice", DEFAULT_TASKRUNNER_SERVICE_POOL_SIZE);
		LOG.info("Starting ExecutorService for TaskRunnner with PoolSize="+poolSize);
		return Executors.newFixedThreadPool(poolSize, new NamedThreadFactory("SocketWorkerTaskRunner"));
	}
	
	@Provides
	@Singleton
	@Named("resultCollectorSuperVisorService")
	private ExecutorService supervisorExecutorService() {
		return Executors.newSingleThreadExecutor(new NamedThreadFactory("ResultCollectorSupervisor"));
	}
	
	int sysProp(String name, int def) {
		String propVal = System.getProperty(name);
		int retVal;
		if (propVal != null) {
			retVal = Integer.parseInt(propVal);
		} else {
			retVal = def;
		}
		LOG.log(Level.CONFIG, "{0}={1}", new Object[] { name, retVal });
		return retVal;
	}

}
