package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AbstractClientSocketMapReduceTest;
import ch.zhaw.mapreduce.plugins.socket.InvalidAgentTaskException;

public class SocketAgentImplTest extends AbstractClientSocketMapReduceTest {
	
	@Test
	public void shouldSetClientIp() {
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory);
		assertEquals(clientIp, sa.getIp());
	}
	
	@Test
	public void shouldRunSmoothly() {
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory);
		sa.helloslave();
	}
	
	@Test
	public void shouldRunTask() throws InvalidAgentTaskException {
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory);
		this.mockery.checking(new Expectations() {{ 
			allowing(aTask).getMapReduceTaskUuid();
			allowing(aTask).getTaskUuid();
			oneOf(trFactory).createTaskRunner(aTask); will(returnValue(taskRunner));
			oneOf(taskRunner).runTask(); will(returnValue(result));
		}});
		assertSame(result, sa.runTask(aTask));
	}
}
