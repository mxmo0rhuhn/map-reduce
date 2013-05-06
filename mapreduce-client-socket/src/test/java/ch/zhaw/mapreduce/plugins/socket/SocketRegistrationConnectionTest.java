package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.WorkerTask;

import de.root1.simon.Lookup;
import de.root1.simon.Registry;
import de.root1.simon.Simon;
import de.root1.simon.annotation.SimonRemote;

public class SocketRegistrationConnectionTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery() {
		{
			setThreadingPolicy(new Synchroniser());
		}
	};

	@Mock
	private RegistrationServer innerRegServer;

	@Mock
	private ClientCallback innerClientCallback;

	private final String name = "regServerName";

	@Test
	public void shouldInvokeCallback() throws Exception {
		RegistrationServer regServer = new RegistrationServerMockWrapper(innerRegServer);
		Registry registry = Simon.createRegistry(39847);
		Lookup lookup = Simon.createNameLookup("localhost", 39847);
		ServerPluginPartNameMeBetter binder = new ServerPluginPartNameMeBetter(regServer, registry, name);
		binder.bind();
		SocketClientBinder client = new SocketClientBinder(lookup, name);
		client.bind();
		this.mockery.checking(new Expectations() {
			{
				oneOf(innerRegServer).register(with(aNonNull(ClientCallback.class)));
			}
		});
		client.invoke(new ClientCallbackTest());
	}

}

@SimonRemote(RegistrationServer.class)
class RegistrationServerMockWrapper implements RegistrationServer {

	private final RegistrationServer mock;

	public RegistrationServerMockWrapper(RegistrationServer mock) {
		this.mock = mock;
	}

	@Override
	public void register(ClientCallback clientCallback) {
		this.mock.register(clientCallback);
	}

}

class ClientCallbackTest implements ClientCallback, Serializable {

	private static final long serialVersionUID = 1700918334639412558L;

	@Override
	public void helloslave() {
		System.out.println("hello, master");
	}

	@Override
	public Object runTask(WorkerTask arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getIp() {
		// TODO Auto-generated method stub
		return null;
	}
}