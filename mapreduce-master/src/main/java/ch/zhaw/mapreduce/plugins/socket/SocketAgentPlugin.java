package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.plugins.AgentPlugin;
import ch.zhaw.mapreduce.plugins.PluginException;

import com.google.inject.Injector;

public class SocketAgentPlugin implements AgentPlugin {

	private ServerPluginPartNameMeBetter binder;

	@Override
	public void start(Injector injector) throws PluginException {
		Injector child = injector.createChildInjector(new SocketConfig());
		this.binder = child.getInstance(ServerPluginPartNameMeBetter.class);
		try {
			this.binder.bind();
		} catch (Exception e) {
			throw new PluginException(e);
		}
	}

	@Override
	public void stop() {
		this.binder.stop();
	}

}