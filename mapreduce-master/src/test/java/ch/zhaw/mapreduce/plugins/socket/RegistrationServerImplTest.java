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
	private ClientCallback callback;
	
	@Test
	public void shouldAcknowledgeAndCreateNewWorker() {
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		RegistrationServerImpl reg = new RegistrationServerImpl(p, swFactory);
		this.mockery.checking(new Expectations() {{ 
			oneOf(callback).acknowledge();
			oneOf(swFactory).createSocketWorker("123.123.234.234", 6756, callback);
		}});
		reg.register("123.123.234.234", 6756, callback);
	}

}
