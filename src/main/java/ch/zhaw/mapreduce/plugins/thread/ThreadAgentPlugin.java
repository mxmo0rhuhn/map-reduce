package ch.zhaw.mapreduce.plugins.thread;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.PluginException;

import com.google.inject.Injector;

public class ThreadAgentPlugin implements AgentPlugin {
	
	@Override
	public void start(Injector parent) throws PluginException {
		Injector child = parent.createChildInjector(new ThreadConfig());
		Worker w = child.getInstance(Worker.class);
		
		Pool p = child.getInstance(Pool.class);
		
		for (int i = 1; i < Runtime.getRuntime().availableProcessors() + 1; i++) {
			System.out.println("worker started");
			p.donateWorker(w);
		}
	}

	@Override
	public void stop() {
	}

}
