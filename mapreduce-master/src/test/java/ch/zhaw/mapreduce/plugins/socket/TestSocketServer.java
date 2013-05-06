package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.registry.MapReduceConfig;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestSocketServer {
	
	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
		ServerPluginPartNameMeBetter server = injector.getInstance(ServerPluginPartNameMeBetter.class);
		server.bind();
	}

}
