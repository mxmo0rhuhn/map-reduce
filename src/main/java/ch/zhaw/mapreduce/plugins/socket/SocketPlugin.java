package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.PluginException;

import com.google.inject.Injector;

public class SocketPlugin implements AgentPlugin {

	@Override
	public void start(Injector injector) throws PluginException {
		Injector child = injector.createChildInjector(new SocketConfig());
		// get instance via guice
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
