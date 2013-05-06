package ch.zhaw.mapreduce.plugins.socket;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestSocketClient {
	
	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new SocketClientConfig());
		SocketClientBinder binder = injector.getInstance(SocketClientBinder.class);
		binder.donateWorker(new TestClientCallback());
	}

}