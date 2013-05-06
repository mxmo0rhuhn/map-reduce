package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import de.root1.simon.annotation.SimonRemote;


@SimonRemote(RegistrationServer.class)
public class RegistrationServerImpl implements RegistrationServer {
	
	private static final Logger LOG = Logger.getLogger(RegistrationServerImpl.class.getName());
	
	private final Pool pool;
	
	private final SocketWorkerFactory factory;
	
	@Inject
	public RegistrationServerImpl(Pool pool, SocketWorkerFactory factory) {
		this.pool = pool;
		this.factory = factory;
	}

	@Override
	public void register(ClientCallback callback) {
		LOG.info("New Worker: " + callback.getIp());
		callback.helloslave();
		Worker worker = this.factory.createSocketWorker(callback);
		this.pool.donateWorker(worker);
	}
}
