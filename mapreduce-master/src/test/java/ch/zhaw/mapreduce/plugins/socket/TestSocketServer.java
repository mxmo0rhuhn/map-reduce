package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.impl.MapWorkerTask;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.root1.simon.Registry;
import de.root1.simon.Simon;

public class TestSocketServer {

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
//		ServerPluginPartNameMeBetter server = injector.getInstance(ServerPluginPartNameMeBetter.class);
//		server.bind();
//
		Pool p = injector.getInstance(Pool.class);
		SocketWorkerFactory swFactory = injector.getInstance(SocketWorkerFactory.class);
//		
//		for (int i = 0; i < 10; i++) {
//			MapWorkerTask mapTask = new MapWorkerTask("myMapReduceId", "myTaskId"+i, new TestMapInstruction(), null, "value"+i);
//			p.enqueueWork(mapTask);
//		}
		
		
		Registry reg = Simon.createRegistry(4753);
		reg.bind("MapReduceSocketMaster", new RegistrationServerImpl(p, swFactory));
		p.enqueueWork(new MapWorkerTask("mrtUuid", "tUuid", new TestMapInstruction(), null, "input"));
	}

}