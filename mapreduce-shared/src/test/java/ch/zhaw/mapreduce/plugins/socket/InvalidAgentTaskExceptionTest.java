package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.*;

import org.junit.Test;

public class InvalidAgentTaskExceptionTest {
	
	@Test
	public void shouldSetCause() {
		Exception cause = new Exception();
		InvalidAgentTaskException iate = new InvalidAgentTaskException(cause);
		assertSame(cause, iate.getCause());
	}
	
	@Test
	public void shouldSetMessage() {
		InvalidAgentTaskException iate = new InvalidAgentTaskException("i fail");
		assertEquals("i fail", iate.getMessage());
	}

}
