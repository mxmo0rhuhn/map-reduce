package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapReduceUtil;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.root1.simon.Lookup;
import de.root1.simon.Simon;

/**
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public class SocketClientStarter {
	
	private static SocketClientBinder binder;
	
	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				binder.release();
			}
		});
	}
	
	public static void main(String[] args) throws Exception {
		String clientIp = MapReduceUtil.getLocalIp();
		String masterip = args[0];
		int masterport = Integer.parseInt(args[1]);
		int nworker = args.length == 3 ? Integer.parseInt(args[2]) : Runtime.getRuntime().availableProcessors() + 1;
		
		Lookup lookup = Simon.createNameLookup(masterip, masterport);
		SocketResultCollector resCollector = (SocketResultCollector) lookup.lookup(SharedSocketConfig.SOCKET_RESULT_COLLECTOR_SIMON_BINDING);
		
		Injector injector = Guice.createInjector(new SocketClientConfig(resCollector, nworker));
		SocketAgentFactory saFactory = injector.getInstance(SocketAgentFactory.class);
		binder = new SocketClientBinder(lookup, SharedSocketConfig.AGENT_REGISTRATOR_SIMON_BINDING);
		
		for (int i = 0; i < nworker; i++) {
			binder.registerAgent(saFactory.createSocketAgent(clientIp));
		}
	}

}
