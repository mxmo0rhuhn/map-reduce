package ch.zhaw.mapreduce.plugins;

import com.google.inject.Injector;

public interface AgentPlugin {
	
	void start(Injector injector) throws PluginException;
	
	void stop();
	
}
