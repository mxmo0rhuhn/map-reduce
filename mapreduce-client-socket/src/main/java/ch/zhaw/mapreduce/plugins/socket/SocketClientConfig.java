package ch.zhaw.mapreduce.plugins.socket;

import static ch.zhaw.mapreduce.MapReduceUtil.loadWithOptionalDefaults;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Singleton;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.PostConstructFeature;
import ch.zhaw.mapreduce.impl.ContextImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentStatistics;
import ch.zhaw.mapreduce.plugins.socket.impl.MapTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.NamedThreadFactory;
import ch.zhaw.mapreduce.plugins.socket.impl.ReduceTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentResultFactoryImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.TaskRunnerFactoryImpl;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;

public final class SocketClientConfig extends AbstractModule {

	private static final Logger LOG = Logger.getLogger(SocketClientConfig.class.getName());

	private final SocketResultCollector resCollector;

	private final int nworker;

	public SocketClientConfig(SocketResultCollector resCollector, int nworker) {
		this.resCollector = resCollector;
		this.nworker = nworker;
	}

	@Override
	protected void configure() {
		install(new SharedSocketConfig());
		bindListener(Matchers.any(), new PostConstructFeature());
		try {
			Names.bindProperties(binder(),
					loadWithOptionalDefaults("client-socket-defaults.properties", "client-socket.properties"));
		} catch (IOException e) {
			addError(e);
		}
		
		bind(TaskRunnerFactory.class).to(TaskRunnerFactoryImpl.class);
		bind(SocketAgentResultFactory.class).to(SocketAgentResultFactoryImpl.class);
		bind(SocketResultCollector.class).toInstance(this.resCollector);
		bind(Context.class).to(ContextImpl.class);
		bind(AgentStatistics.class);

		install(new FactoryModuleBuilder().implement(SocketAgent.class, SocketAgentImpl.class).build(SocketAgentFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, MapTaskRunner.class).build(MapTaskRunnerFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, ReduceTaskRunner.class).build(ReduceTaskRunnerFactory.class));
	}

	@Provides
	@Singleton
	@Named("TaskRunnerService")
	private ExecutorService taskRunnerService() {
		LOG.info("Use Pool of Size " + this.nworker + " for TaskRunnerService");
		return Executors.newFixedThreadPool(this.nworker, new NamedThreadFactory("TaskRunnerService"));
	}

	@Provides
	@Singleton
	@Named("ResultPusherService")
	private ExecutorService resultPusherService() {
		LOG.info("Use Pool of Size " + this.nworker + " for ResultPusherService");
		return Executors.newFixedThreadPool(this.nworker, new NamedThreadFactory("ResultPusherService"));
	}
	
	@Provides
	@Singleton
	@Named("SocketScheduler")
	private ScheduledExecutorService socketscheduler(@Named("SocketSchedulerPoolSize") int poolSize) {
		return Executors.newScheduledThreadPool(poolSize, new NamedThreadFactory("SocketScheduler"));
	}

}
