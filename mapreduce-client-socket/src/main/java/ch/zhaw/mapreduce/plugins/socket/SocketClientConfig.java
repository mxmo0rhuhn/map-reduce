package ch.zhaw.mapreduce.plugins.socket;

import java.net.UnknownHostException;

import com.google.inject.AbstractModule;

import de.root1.simon.Lookup;
import de.root1.simon.Simon;

public final class SocketClientConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(SocketClientBinder.class);
		try {
			// TODO remove logic from guice config
			bind(Lookup.class).toInstance(Simon.createNameLookup("localhost", 4753));
		} catch (UnknownHostException e) {
			addError(e);
		}
	}

}
