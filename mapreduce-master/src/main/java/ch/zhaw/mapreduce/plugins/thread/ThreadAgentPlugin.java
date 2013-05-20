package ch.zhaw.mapreduce.plugins.thread;

import java.util.logging.Logger;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.PluginException;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class ThreadAgentPlugin implements AgentPlugin {

	@Override
	public void start(Injector parent) throws PluginException {
		Injector child = parent.createChildInjector(new ThreadConfig());
		Integer nworkers = child.getInstance(Key.get(Integer.class, Names.named("nWorkers")));
		Pool p = child.getInstance(Pool.class);
		Logger log = child.getInstance(Logger.class);

		log.info("Add " + nworkers + " Workers to Pool");
		for (int i = 0; i < nworkers; i++) {
			Worker w = child.getInstance(Worker.class);
			p.donateWorker(w);
		}
		log.info("Added " + nworkers + " Workers to Pool");
	}

	@Override
	public void stop() {
	}

}
