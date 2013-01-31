package ch.zhaw.mapreduce;

import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.Loader;
import ch.zhaw.mapreduce.plugins.PluginException;
import ch.zhaw.mapreduce.registry.MapReduceConfig;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class Starter {

	public static void main(String[] args) throws PluginException {
		Injector injector = Guice.createInjector(new MapReduceConfig());
		Loader l = injector.getInstance(Loader.class);
		for (AgentPlugin plugin : l.loadPlugins()) {
			plugin.start(injector);
		}
	}

}
