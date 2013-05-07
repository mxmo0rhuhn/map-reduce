package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.impl.InMemoryPersistence;
import ch.zhaw.mapreduce.impl.LocalContext;
import ch.zhaw.mapreduce.plugins.socket.impl.SocketAgentImpl;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

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
		bind(Persistence.class).to(InMemoryPersistence.class);
		
			// TODO remove logic from guice config
			// bind(Lookup.class).toInstance(Simon.createNameLookup(this.masterip, this.masterport));
	}

}
