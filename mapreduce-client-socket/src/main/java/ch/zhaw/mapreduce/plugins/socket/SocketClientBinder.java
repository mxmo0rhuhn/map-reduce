package ch.zhaw.mapreduce.plugins.socket;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

import de.root1.simon.Lookup;

public class SocketClientBinder {

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
	
	public void donateWorker(ClientCallback cb) {
		this.regServer.register(cb);
	}

	public void release() {
		this.lookup.release(this.regServer);
	}

}
