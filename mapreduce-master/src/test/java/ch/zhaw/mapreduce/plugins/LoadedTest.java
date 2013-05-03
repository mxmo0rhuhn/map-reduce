package ch.zhaw.mapreduce.plugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.SocketAgentPlugin;
import ch.zhaw.mapreduce.plugins.thread.ThreadAgentPlugin;

public class LoadedTest {

	@Test
	public void shouldLoadOnePlugin() {
		System.setProperty("shouldLoadOnePluginProperty", "Thread");
		Loader l = new Loader("shouldLoadOnePluginProperty");
		List<AgentPlugin> plugins = l.loadPlugins();
		assertEquals(1, plugins.size());
		assertTrue(plugins.get(0) instanceof ThreadAgentPlugin);
	}

	@Test
	public void shouldLoadMultiplePlugins() {
		System.setProperty("shouldLoadMultiplePluginProperty", "Thread,Socket");
		Loader l = new Loader("shouldLoadMultiplePluginProperty");
		List<AgentPlugin> plugins = l.loadPlugins();
		assertEquals(2, plugins.size());
		assertTrue(plugins.get(0) instanceof ThreadAgentPlugin);
		assertTrue(plugins.get(1) instanceof SocketAgentPlugin);
	}

	@Test
	public void shouldReturnEmptyListIfPropertyIsNotSet() {
		Loader l = new Loader("shouldReturnEmptyListIfPropertyIsNotSet");
		List<AgentPlugin> plugins = l.loadPlugins();
		assertTrue(plugins.isEmpty());
	}

}
