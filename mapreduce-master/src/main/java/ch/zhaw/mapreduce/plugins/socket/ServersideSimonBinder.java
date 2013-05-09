package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.plugins.PluginException;
import de.root1.simon.Registry;

public final class ServersideSimonBinder {

	private static final Logger LOG = Logger.getLogger(ServersideSimonBinder.class.getName());

	private final Registry registrationRegistry;

	private final String agentRegistratorSimonBinding;

	private final AgentRegistrator agentRegistrator;

	private final String resultCollectorSimonBinding;

	private final SocketResultCollector resCollector;

	@Inject
	ServersideSimonBinder(Registry registrationRegistry, AgentRegistrator agentRegistrator,
			@Named("agentRegistratorSimonBinding") String agentRegistratorSimonBinding,
			SocketResultCollector resCollector, @Named("resultCollectorSimonBinding") String resultCollectorSimonBinding) {
		this.registrationRegistry = registrationRegistry;
		this.agentRegistrator = agentRegistrator;
		this.agentRegistratorSimonBinding = agentRegistratorSimonBinding;
		this.resCollector = resCollector;
		this.resultCollectorSimonBinding = resultCollectorSimonBinding;
	}

	public void bind() throws PluginException {
		try {
			this.registrationRegistry.bind(agentRegistratorSimonBinding, agentRegistrator);
			this.registrationRegistry.bind(resultCollectorSimonBinding, resCollector);
			LOG.info(this.agentRegistratorSimonBinding + " bound to " + this.agentRegistrator.getClass().getName());
			LOG.info(this.resultCollectorSimonBinding + " bound to " + this.resCollector.getClass().getName());
		} catch (Exception e) {
			throw new PluginException(e);
		}
	}

	public void stop() {
		LOG.info("Stopping " + ServersideSimonBinder.class.getName());
		this.registrationRegistry.unbind(this.agentRegistratorSimonBinding);
		this.registrationRegistry.unbind(this.resultCollectorSimonBinding);
		this.registrationRegistry.stop();
	}
}
