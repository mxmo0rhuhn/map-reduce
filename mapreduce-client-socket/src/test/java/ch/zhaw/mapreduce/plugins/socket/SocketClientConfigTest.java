package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

import de.root1.simon.Lookup;

public class SocketClientConfigTest {

	@Test
	public void shouldUseIpAndPortIfProvided() {
		Injector injector = Guice.createInjector(new SocketClientConfig("123.234.123.234", 7402));
		Lookup l = injector.getInstance(Lookup.class);
		assertEquals(7402, l.getServerPort());
		assertEquals("123.234.123.234", l.getServerAddress().getHostAddress().toString());
	}

}
