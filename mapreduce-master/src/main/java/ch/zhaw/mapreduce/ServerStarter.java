package ch.zhaw.mapreduce;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
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

	private static final Logger LOG = Logger.getLogger(ServerStarter.class.getName());

	private final List<AgentPlugin> startedPlugins = new LinkedList<AgentPlugin>();

	private final Injector injector;

	private final Loader loader;

	public ServerStarter(Injector injector) {
		this.loader = injector.getInstance(Loader.class);
		this.injector = injector;
	}

	public void start() {
		try {
			for (AgentPlugin plugin : loader.loadPlugins()) {
				plugin.start(injector);
				this.startedPlugins.add(plugin);
				LOG.info("Loaded Plugin " + plugin.getClass().getName());
			}
		} catch (PluginException pe) {
			LOG.log(Level.SEVERE, "Failed to load Start Server", pe);
		}
	}

	public void stop() {
		for (AgentPlugin plugin : this.startedPlugins) {
			plugin.stop();
		}
	}

}
