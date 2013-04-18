package ch.zhaw.mapreduce;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

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

	private final List<AgentPlugin> startedPlugins = new LinkedList<AgentPlugin>();

	private Logger log;

	public void start() {
		Injector injector = Guice.createInjector(new MapReduceConfig());
		this.log = injector.getInstance(Logger.class);
		Loader l = injector.getInstance(Loader.class);
		for (AgentPlugin plugin : l.loadPlugins()) {
			try {
				plugin.start(injector);
				this.startedPlugins.add(plugin);
				this.log.info("Loaded Plugin " + plugin.getClass().getName());
			} catch (PluginException pe) {
				this.log.severe("Failed to load Plugin " + plugin.getClass().getName() + ": " + pe.getMessage());
			}
		}
	}
	
	public void stop() {
		for (AgentPlugin plugin : this.startedPlugins) {
			plugin.stop();
		}
	}

}
