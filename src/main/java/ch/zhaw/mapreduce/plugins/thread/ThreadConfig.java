package ch.zhaw.mapreduce.plugins.thread;

import ch.zhaw.mapreduce.plugins.AgentAdapter;

import com.google.inject.AbstractModule;

public class ThreadConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(AgentAdapter.class).to(ThreadAgentAdapter.class);
	}

}
