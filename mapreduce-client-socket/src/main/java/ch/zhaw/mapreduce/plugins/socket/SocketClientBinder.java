package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

import de.root1.simon.Lookup;

public class SocketClientBinder {
	
	private static final Logger LOG = Logger.getLogger(SocketClientBinder.class.getName());

	private final Lookup lookup;

	private final String masterRegistratorName;

	private RegistrationServer regServer;

	@Inject
	SocketClientBinder(Lookup lookup, @Named("socket.mastername") String masterRegistratorName) {
		this.lookup = lookup;
		this.masterRegistratorName = masterRegistratorName;
	}

	@PostConstruct
	public void bind() throws Exception {
		this.regServer = (RegistrationServer) this.lookup.lookup(masterRegistratorName);
	}
	
	public void registerAgent(SocketAgent agent) {
		LOG.info("Donating Worker to Master");
		this.regServer.register(agent);
	}

	public void release() {
		this.lookup.release(this.regServer);
	}

}
