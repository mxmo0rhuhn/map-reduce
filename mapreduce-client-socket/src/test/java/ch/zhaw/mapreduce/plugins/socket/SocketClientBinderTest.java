package ch.zhaw.mapreduce.plugins.socket;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import de.root1.simon.Lookup;

public class SocketClientBinderTest {
	
	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private Lookup lookup;
	
	@Mock
	private AgentRegistrator boundObject;
	
	@Mock
	private SocketAgent agent;
	
	private final String mastername = "mastername";

	@Test
	public void shouldLookupName() throws Exception {
		SocketClientBinder binder = new SocketClientBinder(lookup, mastername);
		this.mockery.checking(new Expectations() {{ 
			oneOf(lookup).lookup(mastername); will(returnValue(boundObject));
		}});
		binder.bind();
	}
	
	@Test
	public void shouldReleaseBoundObject() throws Exception {
		SocketClientBinder binder = new SocketClientBinder(lookup, mastername);
		this.mockery.checking(new Expectations() {{ 
			oneOf(lookup).lookup(mastername); will(returnValue(boundObject));
			oneOf(lookup).release(boundObject);
		}});
		binder.bind();
		binder.release();
	}
	
	@Test
	public void shouldInvokeOnBoundObject() throws Exception {
		SocketClientBinder binder = new SocketClientBinder(lookup, mastername);
		this.mockery.checking(new Expectations() {{ 
			oneOf(lookup).lookup(mastername); will(returnValue(boundObject));
			oneOf(boundObject).register(agent);
		}});
		binder.bind();
		binder.registerAgent(agent);
	}
	
}
