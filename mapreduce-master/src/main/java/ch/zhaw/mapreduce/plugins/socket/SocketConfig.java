package ch.zhaw.mapreduce.plugins.socket;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import de.root1.simon.Registry;
import de.root1.simon.Simon;

public class SocketConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(RegistrationServer.class).to(RegistrationServerImpl.class);
		bind(Integer.class).annotatedWith(Names.named("socket.masterpoolsize")).toInstance(1); // offizieller SIMON IANA
		bind(String.class).annotatedWith(Names.named("socket.mastername")).toInstance("MapReduceSocketMaster");
		
		bind(ServerPluginPartNameMeBetter.class);
		
		try {
			bind(Registry.class).toInstance(Simon.createRegistry(4753));
		} catch (Exception e) {
			addError(e);
		}
	}

}