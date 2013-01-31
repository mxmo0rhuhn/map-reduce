package ch.zhaw.mapreduce;

import ch.zhaw.mapreduce.plugins.AdapterException;
import ch.zhaw.mapreduce.plugins.AgentAdapter;
import ch.zhaw.mapreduce.plugins.Loader;
import ch.zhaw.mapreduce.plugins.Plugin;
import ch.zhaw.mapreduce.registry.MapReduceConfig;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class Starter {

	public static void main(String[] args) {
		Injector injector = Guice.createInjector(new MapReduceConfig());
		Loader l = injector.getInstance(Loader.class);
		for (Plugin plugin : l.loadPlugins()) {
			AgentAdapter adapter = plugin.createAdapter(injector);
			try {
				adapter.start();
			} catch (AdapterException e) {
				System.err.println("Failed to start Adapter..");
				e.printStackTrace();
			}
		}
	}

}
