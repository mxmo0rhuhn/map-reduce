package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.registry.MapReduceConfig;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.root1.simon.annotation.SimonRemote;

public class TestSocketServer {

	public static void main(String[] args) throws Exception {
		Injector injector = Guice.createInjector(new MapReduceConfig()).createChildInjector(new SocketServerConfig());
		ServerPluginPartNameMeBetter server = injector.getInstance(ServerPluginPartNameMeBetter.class);
		server.bind();

		Pool p = injector.getInstance(Pool.class);
		
		for (int i = 0; i < 1000; i++) {
			MapWorkerTask mapTask = new MapWorkerTask("myMapReduceId", "myTaskId"+i, new TestMapInstruction(), null, "value"+i);
			p.enqueueWork(mapTask);
		}
	}

}

@SimonRemote
class TestMapInstruction implements MapInstruction {

	private static final long serialVersionUID = -5951237460480827687L;

	@Override
	public void map(MapEmitter emitter, String input) {
		emitter.emitIntermediateMapResult("key", input);
	}

}