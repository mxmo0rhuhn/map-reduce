package ch.zhaw.mapreduce.plugins.socket;

import javax.inject.Inject;
import javax.inject.Named;

import de.root1.simon.Lookup;

public class SocketClientBinder {

	private final Lookup lookup;

	private final String masterRegistratorName;

	private RegistrationServer regServer;

	@Inject
	public SocketClientBinder(Lookup lookup, @Named("masterRegistratorName") String masterRegistratorName) {
		this.lookup = lookup;
		this.masterRegistratorName = masterRegistratorName;
	}

	public void bind() throws Exception {
		this.regServer = (RegistrationServer) this.lookup.lookup(masterRegistratorName);
	}
	
	public void invoke(String addr, int port, ClientCallback cb) {
		this.regServer.register(addr, port, cb);
	}

	public void release() {
		this.lookup.release(this.regServer);
	}

}
