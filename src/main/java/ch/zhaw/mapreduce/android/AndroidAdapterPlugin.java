package ch.zhaw.mapreduce.android;

import ch.zhaw.mapreduce.AdapterPlugin;
import ch.zhaw.mapreduce.AgentAdapter;

import com.google.inject.Injector;

public class AndroidAdapterPlugin implements AdapterPlugin {

	@Override
	public AgentAdapter createAdapter(Injector parent) {
		Injector injector = parent.createChildInjector(new AndroidConfig());
		return injector.getInstance(AgentAdapter.class);
	}

}
