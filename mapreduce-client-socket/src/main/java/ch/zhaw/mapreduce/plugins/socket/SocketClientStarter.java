package ch.zhaw.mapreduce.plugins.socket;

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
		Injector injector = Guice.createInjector(new SocketClientConfig(args[0], Integer.parseInt(args[1])));
		binder = injector.getInstance(SocketClientBinder.class);
	}

}
