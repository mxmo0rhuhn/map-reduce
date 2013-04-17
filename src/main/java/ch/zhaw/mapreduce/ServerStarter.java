package ch.zhaw.mapreduce;

import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.Loader;
import ch.zhaw.mapreduce.plugins.PluginException;
import ch.zhaw.mapreduce.registry.MapReduceConfig;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Der ServerStarter lädt die Plugins in einem Server, welche dann verschiedene Worker aktivieren. Somit wird dem
 * MapReduce Framework erst die Fähigkeit beigebracht, Dinge auszuführen.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class ServerStarter {

	public void start() throws PluginException {
		Injector injector = Guice.createInjector(new MapReduceConfig());
		Loader l = injector.getInstance(Loader.class);
		for (AgentPlugin plugin : l.loadPlugins()) {
			plugin.start(injector);
		}
	}

}
