package ch.zhaw.mapreduce.plugins;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * 
 * Dieser Load lädt Plugins von einem vordefinierten System Property. 
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public final class Loader {

	private static final Logger LOG = Logger.getLogger(Loader.class.getName());

	private final String plugins;

	@Inject
	public Loader(@Named("plugins") String plugins) {
		this.plugins = plugins;
	}

	public List<AgentPlugin> loadPlugins() throws PluginException {
		String[] pluginNames = plugins.split(",");
		return loadClasses(pluginNames);
	}

	private List<AgentPlugin> loadClasses(String[] pluginNames) throws PluginException {
		List<AgentPlugin> instances = new LinkedList<AgentPlugin>();
		for (String pluginName : pluginNames) {
			AgentPlugin plugin = instantiate(pluginName);
			if (plugin != null) {
				instances.add(plugin);
				LOG.fine("Plugin " + pluginName + " found");
			}
		}
		return instances;
	}

	/**
	 * Alle Plugins werden nach dem gleichen Schema deklariert. e.g. pluginName = Android --> className =
	 * ch.zhaw.mapreduce.plugins.android.AndroidPlugin
	 * 
	 * @param pluginName
	 * @return
	 */
	private AgentPlugin instantiate(String pluginName) throws PluginException {
		String baseName = Loader.class.getPackage().getName();
		String className = String.format("%s.%s.%sAgentPlugin", baseName, pluginName.toLowerCase(), pluginName);
		try {
			Class<AgentPlugin> klass = (Class<AgentPlugin>) Class.forName(className);
			return klass.newInstance();
		} catch (Exception e) {
			throw new PluginException(e);
		}
	}
}
