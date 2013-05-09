package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.plugins.socket.AgentRegistrator;
import ch.zhaw.mapreduce.plugins.socket.SocketAgent;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;
import ch.zhaw.mapreduce.plugins.socket.SocketWorker;
import ch.zhaw.mapreduce.plugins.socket.SocketWorkerFactory;
import de.root1.simon.annotation.SimonRemote;


@SimonRemote(AgentRegistrator.class)
public class AgentRegistratorImpl implements AgentRegistrator {
	
	private static final Logger LOG = Logger.getLogger(AgentRegistratorImpl.class.getName());
	
	private final Pool pool;
	
	private final SocketWorkerFactory factory;
	
	private final SocketResultCollector resultsCollector;
	
	@Inject
	public AgentRegistratorImpl(Pool pool, SocketWorkerFactory factory, SocketResultCollector resultsCollector) {
		this.pool = pool;
		this.factory = factory;
		this.resultsCollector = resultsCollector;
	}

	@Override
	public void register(SocketAgent agent) {
		LOG.entering(getClass().getName(), "register", agent);
		agent.helloslave();
		SocketWorker worker = this.factory.createSocketWorker(agent, this.resultsCollector);
		this.pool.donateWorker(worker);
		LOG.exiting(getClass().getName(), "register");
	}
}
