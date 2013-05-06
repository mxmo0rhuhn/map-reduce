package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.impl.MapWorkerTask;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestSocketServer {

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
		ServerPluginPartNameMeBetter server = injector.getInstance(ServerPluginPartNameMeBetter.class);
		server.bind();

		Pool p = injector.getInstance(Pool.class);
		
		for (int i = 0; i < 10; i++) {
			MapWorkerTask mapTask = new MapWorkerTask("myMapReduceId", "myTaskId"+i, new TestMapInstruction(), null, "value"+i);
			p.enqueueWork(mapTask);
		}
	}

}