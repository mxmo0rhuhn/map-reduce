package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapReduceUtil;

import com.google.inject.Guice;

import de.root1.simon.Lookup;
import de.root1.simon.Simon;

public class TestSocketClient {

	public static void main(String[] args) throws Exception {
		// SocketClientStarter.main(new String[] {"localhost", "4753", "1"});
		Lookup lookup = Simon.createNameLookup("localhost", 4753);
		RegistrationServer srv = (RegistrationServer) lookup.lookup("MapReduceMasterName");
		
		srv.register(Guice.createInjector(new SocketClientConfig()).getInstance(SocketAgentFactory.class)
				.createSocketAgent(MapReduceUtil.getLocalIp()));
	}

}