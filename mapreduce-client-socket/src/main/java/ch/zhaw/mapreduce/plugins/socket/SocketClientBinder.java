package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import de.root1.simon.Lookup;

public class SocketClientBinder {
	
	private static final Logger LOG = Logger.getLogger(SocketClientBinder.class.getName());

	private final Lookup lookup;

	private final String agentRegistratorSimonBinding;

	private AgentRegistrator regServer;

	SocketClientBinder(Lookup lookup, String agentRegistratorSimonBinding) {
		this.lookup = lookup;
		this.agentRegistratorSimonBinding = agentRegistratorSimonBinding;
	}

	public void bind() throws Exception {
		this.regServer = (AgentRegistrator) this.lookup.lookup(agentRegistratorSimonBinding);
	}
	
	public void registerAgent(SocketAgent agent) {
		LOG.entering(getClass().getName(), "registerAgent", agent);
		this.regServer.register(agent);
		LOG.exiting(getClass().getName(), "registerAgent", agent);
	}

	public void release() {
		this.lookup.release(this.regServer);
	}

}
