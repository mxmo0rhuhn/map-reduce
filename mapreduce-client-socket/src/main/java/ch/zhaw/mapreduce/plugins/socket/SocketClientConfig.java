package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.impl.InMemoryPersistence;
import ch.zhaw.mapreduce.impl.LocalContext;
import ch.zhaw.mapreduce.plugins.socket.impl.MapTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.ReduceTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentResultFactoryImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.TaskRunnerFactoryImpl;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public final class SocketClientConfig extends AbstractModule {

	private static final Logger LOG = Logger.getLogger(SocketClientConfig.class.getName());

	private static final long DEFAULT_TASK_RUN_TIMEOUT = 10000;

	private final SocketResultCollector resCollector;

	private final int nworker;

	SocketClientConfig(SocketResultCollector resCollector, int nworker) {
		this.resCollector = resCollector;
		this.nworker = nworker;
	}

	@Override
	protected void configure() {
		install(new SharedSocketConfig());

		bind(Persistence.class).to(InMemoryPersistence.class);
		bind(TaskRunnerFactory.class).to(TaskRunnerFactoryImpl.class);
		bind(SocketAgentResultFactory.class).to(SocketAgentResultFactoryImpl.class);
		bind(SocketResultCollector.class).toInstance(this.resCollector);

		bind(Long.class).annotatedWith(Names.named("taskRunTimeout")).toInstance(sysProp("taskRunTimeout", DEFAULT_TASK_RUN_TIMEOUT));

		install(new FactoryModuleBuilder().implement(SocketAgent.class, SocketAgentImpl.class).build(SocketAgentFactory.class));
		install(new FactoryModuleBuilder().implement(Context.class, LocalContext.class).build(ContextFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, MapTaskRunner.class).build(MapTaskRunnerFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, ReduceTaskRunner.class).build(ReduceTaskRunnerFactory.class));
	}

	@Provides
	@Singleton
	@Named("taskRunnerService")
	private ExecutorService taskRunnerService() {
		LOG.info("Use Pool of Size " + this.nworker + " for TaskRunnerService");
		return Executors.newFixedThreadPool(this.nworker);
	}

	@Provides
	@Singleton
	@Named("resultPusherService")
	private ExecutorService resultPusherService() {
		int poolSize = (int) sysProp("resultPusherService", this.nworker);
		LOG.info("Use Pool of Size " + poolSize + " for ResultPusherService");
		return Executors.newFixedThreadPool(poolSize);
	}

	long sysProp(String name, long def) {
		String propVal = System.getProperty(name);
		long retVal;
		if (propVal != null) {
			retVal = Long.parseLong(propVal);
		} else {
			retVal = def;
		}
		LOG.log(Level.CONFIG, "{0}={1}", new Object[] { name, retVal });
		return retVal;
	}

}
