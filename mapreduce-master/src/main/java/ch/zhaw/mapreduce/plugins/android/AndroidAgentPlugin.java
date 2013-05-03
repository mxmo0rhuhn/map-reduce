//package ch.zhaw.mapreduce.plugins.android;
//
//import java.io.IOException;
//
//import ch.zhaw.mapreduce.plugins.AgentPlugin;
//import ch.zhaw.mapreduce.plugins.PluginException;
//
//import com.google.inject.Injector;
//
//public class AndroidAgentPlugin implements AgentPlugin {
//	
//	private AndroidAgentAdapter adapter;
//
//	@Override
//	public void start(Injector injector) throws PluginException {
//		Injector child = injector.createChildInjector(new AndroidConfig());
//		adapter = child.getInstance(AndroidAgentAdapter.class);
//		try {
//			adapter.start();
//		} catch (IOException io) {
//			throw new PluginException(io);
//		}
//	}
//
//	@Override
//	public void stop() {
//		adapter.stop();
//	}
//
//}
