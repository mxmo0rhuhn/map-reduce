package ch.zhaw.mapreduce.plugins.socket;

import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.Pool;

public class RegistrationServerImplTest {
	
	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private SocketWorkerFactory swFactory;
	
	@Mock
	private SocketAgent agent;
	
	@Test
	public void shouldAcknowledgeAndCreateNewWorker() {
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		RegistrationServerImpl reg = new RegistrationServerImpl(p, swFactory);
		this.mockery.checking(new Expectations() {{ 
			oneOf(agent).getIp();
			oneOf(agent).helloslave();
			oneOf(swFactory).createSocketWorker(agent);
		}});
		reg.register(agent);
	}

}
