package ch.zhaw.mapreduce.plugins.socket;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/**
 * Shared Socket config für Client und Server.
 * 
 * @author Reto Hablützel (rethab)
 *
 */
public class SharedSocketConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(String.class).annotatedWith(Names.named("socket.mastername")).toInstance("MapReduceSocketMaster");
	}

}
