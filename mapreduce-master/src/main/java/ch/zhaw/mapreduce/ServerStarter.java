package ch.zhaw.mapreduce;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.Loader;
import ch.zhaw.mapreduce.plugins.PluginException;

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
	
	private final Injector injector;

	private final Logger log;
	
	private final Loader loader;
	
	public ServerStarter(Injector injector) {
		this.log = injector.getInstance(Logger.class);
		this.loader = injector.getInstance(Loader.class);
		this.injector = injector;
	}

	public void start() {
		for (AgentPlugin plugin : loader.loadPlugins()) {
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
