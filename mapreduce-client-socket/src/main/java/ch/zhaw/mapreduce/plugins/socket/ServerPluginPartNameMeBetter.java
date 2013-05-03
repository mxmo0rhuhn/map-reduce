package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.plugins.PluginException;
import de.root1.simon.Registry;

public final class ServerPluginPartNameMeBetter {

	private static final Logger LOG = Logger.getLogger(ServerPluginPartNameMeBetter.class.getName());

	private final RegistrationServer registrationServer;

	private final Registry registrationRegistry;

	private final String name;

	@Inject
	public ServerPluginPartNameMeBetter(RegistrationServer registartionServer, Registry registrationRegistry,
			@Named("socket.mastername") String name) {
		this.registrationServer = registartionServer;
		this.registrationRegistry = registrationRegistry;
		this.name = name;
	}

	public void bind() throws PluginException {
		try {
			this.registrationRegistry.bind(this.name, registrationServer);
			LOG.info("Registration Server started on with name " + this.name);
		} catch (Exception e) {
			throw new PluginException(e);
		}
	}

	public void stop() {
		LOG.info("Registration Server stopping...");
		this.registrationRegistry.unbind(this.name);
		this.registrationRegistry.stop();
		LOG.info("Registration Server stopped");
	}
}
