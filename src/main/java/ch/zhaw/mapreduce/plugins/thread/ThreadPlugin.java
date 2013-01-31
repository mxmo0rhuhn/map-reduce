package ch.zhaw.mapreduce.plugins.thread;

import ch.zhaw.mapreduce.plugins.AgentAdapter;
import ch.zhaw.mapreduce.plugins.Plugin;

import com.google.inject.Injector;

public class ThreadPlugin implements Plugin {

	@Override
	public AgentAdapter createAdapter(Injector parent) {
		Injector child = parent.createChildInjector(new ThreadConfig());
		return child.getInstance(AgentAdapter.class);
	}

}
