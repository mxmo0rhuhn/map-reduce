package ch.zhaw.mapreduce.plugins.socket;

import java.net.UnknownHostException;

import com.google.inject.AbstractModule;

import de.root1.simon.Lookup;
import de.root1.simon.Simon;

public final class SocketClientConfig extends AbstractModule {
	
	private final String masterip;
	
	private final int masterport;
	
	SocketClientConfig(String masterip, int masterport) {
		this.masterip = masterip;
		this.masterport = masterport;
	}
	
	public SocketClientConfig() {
		this("localhost", 4753); // IANA SIMON port
	}

	@Override
	protected void configure() {
		install(new SharedSocketConfig());
		bind(SocketClientBinder.class);
		
		try {
			// TODO remove logic from guice config
			bind(Lookup.class).toInstance(Simon.createNameLookup(this.masterip, this.masterport));
		} catch (UnknownHostException e) {
			addError(e);
		}
	}

}
