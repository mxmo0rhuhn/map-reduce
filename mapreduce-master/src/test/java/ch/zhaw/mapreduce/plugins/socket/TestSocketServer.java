package ch.zhaw.mapreduce.plugins.socket;

import java.util.logging.Logger;

import ch.zhaw.mapreduce.MapReduceConfig;
import ch.zhaw.mapreduce.MapReduceUtil;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.impl.MapWorkerTask;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.root1.simon.Registry;

public class TestSocketServer {

	private static final Logger LOG = Logger.getLogger(TestSocketServer.class.getName());

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
		
		AgentRegistrator agentRegistrator = injector.getInstance(AgentRegistrator.class);
		SocketResultCollector resultCollector = injector.getInstance(SocketResultCollector.class);
		Pool p = injector.getInstance(Pool.class);

		LOG.info("I, " + MapReduceUtil.getLocalIp() + ", am thee Master and thou shalt be my Slaves!");
		
		Registry reg = injector.getInstance(Registry.class);
		reg.bind(SharedSocketConfig.AGENT_REGISTRATOR_SIMON_BINDING, agentRegistrator);
		reg.bind(SharedSocketConfig.SOCKET_RESULT_COLLECTOR_SIMON_BINDING, resultCollector);
		
		for (int i = 0; i < 1000000; i++) {
			p.enqueueTask(new MapWorkerTask("mrtUuid", "tUuid" + i, new TestMapInstruction(), null, "input"));
		}
	}

}