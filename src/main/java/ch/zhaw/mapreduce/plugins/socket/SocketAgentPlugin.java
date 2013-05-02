package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.PluginException;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import de.root1.simon.Registry;
import de.root1.simon.Simon;

public class SocketAgentPlugin implements AgentPlugin {

	private Logger log;

	private Registry registry;

	private String name;

	@Override
	public void start(Injector injector) throws PluginException {
		Injector child = injector.createChildInjector(new SocketConfig());
		this.log = child.getInstance(Logger.class);
		RegistrationServer registration = child.getInstance(RegistrationServer.class);
		int port = child.getInstance(Key.get(Integer.class, Names.named("socket.masterport")));
		int poolSize = child.getInstance(Key.get(Integer.class, Names.named("socket.masterpoolsize")));
		this.name = child.getInstance(Key.get(String.class, Names.named("socket.mastername")));
		
		Simon.setWorkerThreadPoolSize(poolSize);
		try {
			this.registry = Simon.createRegistry(port);
			this.registry.bind(this.name, registration);
			this.log.info("Registration Server started on port " + port + " with name " + this.name);
		} catch (Exception e) {
			throw new PluginException(e);
		}
	}

	@Override
	public void stop() {
		this.log.info("Registration Server stopping...");
		this.registry.unbind(this.name);
		this.registry.stop();
		this.log.info("Registration Server stopped");
	}

}
