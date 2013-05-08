package ch.zhaw.mapreduce.plugins.socket;


public class TestSocketClient {

	public static void main(String[] args) throws Exception {
		 SocketClientStarter.main(new String[] {"localhost", "4753", "5"});
//		Lookup lookup = Simon.createNameLookup(args.length == 1 ? args[0] : "localhost", 4753);
//		RegistrationServer srv = (RegistrationServer) lookup.lookup("MapReduceMasterName");
//		
//		srv.register(Guice.createInjector(new SocketClientConfig()).getInstance(SocketAgentFactory.class)
//				.createSocketAgent(MapReduceUtil.getLocalIp()));
	}

}