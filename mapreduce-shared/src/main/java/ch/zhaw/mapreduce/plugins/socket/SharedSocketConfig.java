package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.PostConstructFeature;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;

/**
 * Shared Socket config für Client und Server.
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public class SharedSocketConfig extends AbstractModule {
	
	public static String AGENT_REGISTRATOR_SIMON_BINDING = "MapReduceAgentRegistrator";
	
	public static String SOCKET_RESULT_COLLECTOR_SIMON_BINDING = "MapReduceResultCollector";

	@Override
	protected void configure() {
		bind(String.class).annotatedWith(Names.named("agentRegistratorSimonBinding")).toInstance(AGENT_REGISTRATOR_SIMON_BINDING);
		bind(String.class).annotatedWith(Names.named("resultCollectorSimonBinding")).toInstance(SOCKET_RESULT_COLLECTOR_SIMON_BINDING);
		bindListener(Matchers.any(), new PostConstructFeature());
	}

}
