package ch.zhaw.mapreduce.plugins;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
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

	@Inject
	private Logger logger;

	private final String propName;

	@Inject
	public Loader(@Named("plugins.property") String propName) {
		this.propName = propName;
	}

	public List<AgentPlugin> loadPlugins() {
		String props = System.getProperty(propName);
		if (props == null) {
			logger.warning("No plugins found");
			return Collections.emptyList();
		}
		String[] pluginNames = props.split(",");
		return loadClasses(pluginNames);
	}

	private List<AgentPlugin> loadClasses(String[] pluginNames) {
		List<AgentPlugin> instances = new LinkedList<AgentPlugin>();
		for (String pluginName : pluginNames) {
			AgentPlugin plugin = instantiate(pluginName);
			if (plugin != null) {
				instances.add(plugin);
				logger.config("Plugin " + pluginName + " loaded");
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
	private AgentPlugin instantiate(String pluginName) {
		String baseName = Loader.class.getPackage().getName();
		String className = String.format("%s.%s.%sAgentPlugin", baseName, pluginName.toLowerCase(), pluginName);
		try {
			Class<AgentPlugin> klass = (Class<AgentPlugin>) Class.forName(className);
			return klass.newInstance();
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Failed to load Plugin " + className, e);
			return null;
		}
	}
}
