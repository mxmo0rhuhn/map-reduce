package ch.zhaw.mapreduce.plugins;

import com.google.inject.Injector;

public interface Plugin {
	
	AgentAdapter createAdapter(Injector parent);
	
}
