package ch.zhaw.mapreduce;

import com.google.inject.Injector;

public interface AdapterPlugin {
	
	AgentAdapter createAdapter(Injector parent);
	
}
