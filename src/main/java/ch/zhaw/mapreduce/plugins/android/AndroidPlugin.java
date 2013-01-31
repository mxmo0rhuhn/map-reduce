package ch.zhaw.mapreduce.plugins.android;

import ch.zhaw.mapreduce.plugins.AgentAdapter;
import ch.zhaw.mapreduce.plugins.Plugin;

import com.google.inject.Injector;

public class AndroidPlugin implements Plugin {

	@Override
	public AgentAdapter createAdapter(Injector parent) {
		Injector injector = parent.createChildInjector(new AndroidConfig());
		return injector.getInstance(AgentAdapter.class);
	}

}
