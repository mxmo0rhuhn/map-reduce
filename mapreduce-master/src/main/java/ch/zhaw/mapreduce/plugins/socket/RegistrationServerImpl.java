package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Pool;
import de.root1.simon.annotation.SimonRemote;


@SimonRemote(RegistrationServer.class)
public class RegistrationServerImpl implements RegistrationServer {
	
	private static final Logger LOG = Logger.getLogger(RegistrationServerImpl.class.getName());
	
	private final Pool pool;
	
	@Inject
	public RegistrationServerImpl(Pool pool) {
		this.pool = pool;
	}

	@Override
	public void register(String ip, int port, ClientCallback clientCallback) {
		LOG.info("New Worker: " + ip + ":" + port);
		// TODO erstelle socket worker 
		// adde zu pool
		clientCallback.acknowledge();
	}


}
