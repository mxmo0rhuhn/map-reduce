package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.WorkerTask;

public class SocketAgentImplTest {
	
	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private ContextFactory ctxFactory;
	
	@Mock
	private WorkerTask task;
	
	@Mock
	private Context ctx;
	
	private final String mrUuid = "mapreducetaskuuuid";
	
	private final String taskUuid = "taskUuuid";
	
	private final List<KeyValuePair> mapResult = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key", "value")});
	
	private final List<String> reduceResult = Arrays.asList(new String[]{"redRes"});
	
	@Test
	public void shouldSetClientIp() {
		SocketAgentImpl sa = new SocketAgentImpl("123.123.234.124", ctxFactory);
		assertEquals("123.123.234.124", sa.getIp());
	}
	
	@Test
	public void shouldRunTaskWithContext() {
		SocketAgentImpl sa = new SocketAgentImpl("123.123.234.124", ctxFactory);
		this.mockery.checking(new Expectations() {{ 
			oneOf(task).getMapReduceTaskUuid(); will(returnValue(mrUuid));
			oneOf(task).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(ctxFactory).createContext(mrUuid, taskUuid); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			oneOf(ctx).getMapResult(); will(returnValue(mapResult));
		}});
		assertSame(mapResult, sa.runTask(task));
	}
	
	@Test
	public void shouldReturnMapValueIfComputed() {
		SocketAgentImpl sa = new SocketAgentImpl("123.123.234.124", ctxFactory);
		this.mockery.checking(new Expectations() {{ 
			oneOf(task).getMapReduceTaskUuid(); will(returnValue(mrUuid));
			oneOf(task).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(ctxFactory).createContext(mrUuid, taskUuid); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			oneOf(ctx).getMapResult(); will(returnValue(mapResult));
		}});
		assertSame(mapResult, sa.runTask(task));
	}
	
	@Test
	public void shouldReturnReduceValueIfComputed() {
		SocketAgentImpl sa = new SocketAgentImpl("123.123.234.124", ctxFactory);
		this.mockery.checking(new Expectations() {{ 
			oneOf(task).getMapReduceTaskUuid(); will(returnValue(mrUuid));
			oneOf(task).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(ctxFactory).createContext(mrUuid, taskUuid); will(returnValue(ctx));
			oneOf(task).runTask(ctx);
			oneOf(ctx).getMapResult(); will(returnValue(null));
			oneOf(ctx).getReduceResult(); will(returnValue(reduceResult));
		}});
		assertSame(reduceResult, sa.runTask(task));
	}
	
	@Test
	public void shouldRunSmoothly() {
		SocketAgentImpl sa = new SocketAgentImpl("123.123.234.124", ctxFactory);
		sa.helloslave();
	}
}
