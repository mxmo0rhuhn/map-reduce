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
		Injector injector = Guice.createInjector(new SocketClientConfig(args[0], Integer.parseInt(args[1])));
		SocketAgentFactory saFactory = injector.getInstance(SocketAgentFactory.class);
		binder = injector.getInstance(SocketClientBinder.class);
		binder.donateWorker(saFactory.createSocketAgent(clientIp));
		binder.donateWorker(saFactory.createSocketAgent(clientIp));
	}

}
