package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapReduceUtil;

import com.google.inject.Guice;
import com.google.inject.Injector;

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
		
		Injector injector = Guice.createInjector(new SocketClientConfig(masterip, masterport));
		SocketAgentFactory saFactory = injector.getInstance(SocketAgentFactory.class);
		binder = injector.getInstance(SocketClientBinder.class);
		
		for (int i = 0; i < nworker; i++) {
			binder.registerAgent(saFactory.createSocketAgent(clientIp));
		}
	}

}
