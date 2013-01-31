package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.plugins.AgentAdapter;
import ch.zhaw.mapreduce.plugins.Plugin;

import com.google.inject.Injector;

public class SocketPlugin implements Plugin {

	@Override
	public AgentAdapter createAdapter(Injector parent) {
		Injector child = parent.createChildInjector(new SocketConfig());
		return child.getInstance(AgentAdapter.class);
	}

}
