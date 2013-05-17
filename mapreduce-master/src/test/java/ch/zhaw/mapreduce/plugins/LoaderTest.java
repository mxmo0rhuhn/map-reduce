package ch.zhaw.mapreduce.plugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.SocketAgentPlugin;
import ch.zhaw.mapreduce.plugins.thread.ThreadAgentPlugin;

public class LoaderTest {

	@Test
	public void shouldLoadOnePlugin() throws PluginException {
		Loader l = new Loader("Thread");
		List<AgentPlugin> plugins = l.loadPlugins();
		assertEquals(1, plugins.size());
		assertTrue(plugins.get(0) instanceof ThreadAgentPlugin);
	}

	@Test
	public void shouldLoadMultiplePlugins() throws PluginException {
		Loader l = new Loader("Thread,Socket");
		List<AgentPlugin> plugins = l.loadPlugins();
		assertEquals(2, plugins.size());
		assertTrue(plugins.get(0) instanceof ThreadAgentPlugin);
		assertTrue(plugins.get(1) instanceof SocketAgentPlugin);
	}

	@Test(expected=PluginException.class)
	public void shouldThrowPluginExceptionIfNoneAreConfigured() throws PluginException {
		Loader l = new Loader("");
		List<AgentPlugin> plugins = l.loadPlugins();
		assertTrue(plugins.isEmpty());
	}
	
	@Test(expected=Exception.class)
	public void shouldThrowExceptionIfNoneAreConfiguredNull() throws PluginException {
		Loader l = new Loader(null);
		List<AgentPlugin> plugins = l.loadPlugins();
		assertTrue(plugins.isEmpty());
	}

}
