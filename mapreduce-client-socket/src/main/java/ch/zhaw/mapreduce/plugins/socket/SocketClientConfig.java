package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.impl.InMemoryPersistence;
import ch.zhaw.mapreduce.impl.LocalContext;
import ch.zhaw.mapreduce.plugins.socket.impl.MapTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.ReduceTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentResultImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.TaskRunnerFactoryImpl;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public final class SocketClientConfig extends AbstractModule {
	
	private final SocketResultCollector resCollector;
	
	SocketClientConfig(SocketResultCollector resCollector){
		this.resCollector = resCollector;
	}

	@Override
	protected void configure() {
		install(new SharedSocketConfig());
		
		bind(Persistence.class).to(InMemoryPersistence.class);
		bind(TaskRunnerFactory.class).to(TaskRunnerFactoryImpl.class);
		bind(SocketResultCollector.class).toInstance(this.resCollector);
		
		bind(Long.class).annotatedWith(Names.named("taskRunTimeout")).toInstance(sysProp("taskRunTimeout", 10000));
		bind(ExecutorService.class).annotatedWith(Names.named("resultPusherService")).toInstance(Executors.newSingleThreadExecutor());
		bind(ExecutorService.class).annotatedWith(Names.named("taskRunnerService")).toInstance(Executors.newSingleThreadExecutor());;
		
		install(new FactoryModuleBuilder().implement(SocketAgent.class, SocketAgentImpl.class).build(SocketAgentFactory.class));
		install(new FactoryModuleBuilder().implement(SocketAgentResult.class, SocketAgentResultImpl.class).build(SocketAgentResultFactory.class));
		install(new FactoryModuleBuilder().implement(Context.class, LocalContext.class).build(ContextFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, MapTaskRunner.class).build(MapTaskRunnerFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, ReduceTaskRunner.class).build(ReduceTaskRunnerFactory.class));
	}
	
	long sysProp(String name, long def) {
		String propVal = System.getProperty(name);
		if (propVal != null) {
			return Long.parseLong(propVal);
		} else {
			return def;
		}
	}

}
