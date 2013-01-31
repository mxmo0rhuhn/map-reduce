package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.plugins.AgentAdapter;

import com.google.inject.AbstractModule;

public class SocketConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(AgentAdapter.class).to(SocketAdapter.class);
	}

}
