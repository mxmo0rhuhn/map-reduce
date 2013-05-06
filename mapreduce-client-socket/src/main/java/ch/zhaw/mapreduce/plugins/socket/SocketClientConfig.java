package ch.zhaw.mapreduce.plugins.socket;

import java.net.UnknownHostException;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.impl.FilePersistence;
import ch.zhaw.mapreduce.impl.InMemoryPersistence;
import ch.zhaw.mapreduce.impl.LocalContext;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

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
		bind(SocketClientBinder.class);
		install(new FactoryModuleBuilder().implement(ContextFactory.class, ContextFactory.class).build(SocketAgentFactory.class));
		install(new FactoryModuleBuilder().build(ContextFactory.class));
		bind(Context.class).to(LocalContext.class);
		bind(Persistence.class).to(InMemoryPersistence.class);
		
		try {
			// TODO remove logic from guice config
			bind(Lookup.class).toInstance(Simon.createNameLookup(this.masterip, this.masterport));
		} catch (UnknownHostException e) {
			addError(e);
		}
	}

}
