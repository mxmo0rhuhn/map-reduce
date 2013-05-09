package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.MapReduceUtil;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.plugins.socket.impl.AgentRegistratorImpl;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.root1.simon.Registry;
import de.root1.simon.Simon;

public class TestSocketServer {

	private static final Logger LOG = Logger.getLogger(TestSocketServer.class.getName());

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
		// ServerPluginPartNameMeBetter server = injector.getInstance(ServerPluginPartNameMeBetter.class);
		// server.bind();
		//
		Pool p = injector.getInstance(Pool.class);
		SocketWorkerFactory swFactory = injector.getInstance(SocketWorkerFactory.class);
		SocketResultCollector resCollector = injector.getInstance(SocketResultCollector.class);
		//
		// for (int i = 0; i < 10; i++) {
		// MapWorkerTask mapTask = new MapWorkerTask("myMapReduceId", "myTaskId"+i, new TestMapInstruction(), null,
		// "value"+i);
		// p.enqueueWork(mapTask);
		// }

		LOG.info("I, " + MapReduceUtil.getLocalIp() + ", am thee Master and thou shalt be my Slaves!");
		Registry reg = Simon.createRegistry(4753);
		reg.bind("MapReduceSocketMaster", new AgentRegistratorImpl(p, swFactory, resCollector));
		for (int i = 0; i < 10000; i++) {
			p.enqueueWork(new MapWorkerTask("mrtUuid", "tUuid" + i, new TestMapInstruction(), null, "input"));
		}
	}

}