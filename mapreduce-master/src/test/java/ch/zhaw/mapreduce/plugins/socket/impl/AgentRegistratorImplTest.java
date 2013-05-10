package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.plugins.socket.AbstractMapReduceMasterSocketTest;
import ch.zhaw.mapreduce.plugins.socket.SocketWorker;

public class AgentRegistratorImplTest extends AbstractMapReduceMasterSocketTest {
	
	@Test
	public void shouldAcknowledgeAndCreateNewWorker() {
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		AgentRegistratorImpl reg = new AgentRegistratorImpl(p, swFactory, resCollector);
		final SocketWorker sw = new SocketWorker(sAgent, execMock, p, atFactory, resCollector);
		this.mockery.checking(new Expectations() {{ 
			oneOf(sAgent).helloslave();
			oneOf(swFactory).createSocketWorker(sAgent, resCollector); will(returnValue(sw));
		}});
		reg.register(sAgent);
	}

}
