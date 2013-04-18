package ch.zhaw.mapreduce.plugins.socket;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class SocketConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(RegistrationServer.class).to(RegistrationServerImpl.class);
		bind(Integer.class).annotatedWith(Names.named("socket.masterport")).toInstance(4753); // offizieller SIMON IANA
		bind(Integer.class).annotatedWith(Names.named("socket.masterpoolsize")).toInstance(1); // offizieller SIMON IANA
		bind(String.class).annotatedWith(Names.named("socket.mastername")).toInstance("MapReduceSocketMaster");
	}

}
