package ch.zhaw.mapreduce.plugins.socket;

import java.net.UnknownHostException;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.impl.InMemoryPersistence;
import ch.zhaw.mapreduce.impl.LocalContext;
import ch.zhaw.mapreduce.plugins.socket.impl.MapTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.ReduceTaskRunner;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketTaskResultImpl;
import ch.zhaw.mapreduce.plugins.socket.impl.TaskRunnerFactoryImpl;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import de.root1.simon.Lookup;
import de.root1.simon.Simon;

public final class SocketClientConfig extends AbstractModule {

	private final String masterip;

	private final int masterport;

	SocketClientConfig(String masterip, int masterport) {
		this.masterip = masterip;
		this.masterport = masterport;
	}

	SocketClientConfig() {
		this("localhost", 4753); // IANA SIMON port
	}

	@Override
	protected void configure() {
		install(new SharedSocketConfig());
		// bind(SocketClientBinder.class);
		install(new FactoryModuleBuilder().implement(SocketAgent.class, SocketAgentImpl.class).build(SocketAgentFactory.class));
		install(new FactoryModuleBuilder().implement(Context.class, LocalContext.class).build(ContextFactory.class));
		install(new FactoryModuleBuilder().implement(SocketTaskResult.class, SocketTaskResultImpl.class).build(SocketTaskResultFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, MapTaskRunner.class).build(MapTaskRunnerFactory.class));
		install(new FactoryModuleBuilder().implement(TaskRunner.class, ReduceTaskRunner.class).build(ReduceTaskRunnerFactory.class));
		
		bind(Persistence.class).to(InMemoryPersistence.class);

		bind(TaskRunnerFactory.class).to(TaskRunnerFactoryImpl.class);

		// TODO remove logic from guice config
		try {
			bind(Lookup.class).toInstance(Simon.createNameLookup(this.masterip, this.masterport));
		} catch (UnknownHostException e) {
			addError(e);
		}
	}

}
