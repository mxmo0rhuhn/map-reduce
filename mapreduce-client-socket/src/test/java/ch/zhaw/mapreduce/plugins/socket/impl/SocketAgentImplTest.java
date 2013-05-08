package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Arrays;
import java.util.List;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.plugins.socket.TaskRunnerFactory;

public class SocketAgentImplTest {
	
	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private WorkerTask task;
	
	@Mock
	private Context ctx;
	
	@Mock
	private TaskRunnerFactory trFactory;
	
	private final String mrUuid = "mapreducetaskuuuid";
	
	private final String taskUuid = "taskUuuid";
	
	private final List<KeyValuePair> mapResult = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key", "value")});
	
	private final List<String> reduceResult = Arrays.asList(new String[]{"redRes"});
	
	@Test
	public void shouldSetClientIp() {
		SocketAgentImpl sa = new SocketAgentImpl("123.123.234.124", trFactory);
		assertEquals("123.123.234.124", sa.getIp());
	}
	
	@Test
	public void shouldRunSmoothly() {
		SocketAgentImpl sa = new SocketAgentImpl("123.123.234.124", trFactory);
		sa.helloslave();
	}
}
