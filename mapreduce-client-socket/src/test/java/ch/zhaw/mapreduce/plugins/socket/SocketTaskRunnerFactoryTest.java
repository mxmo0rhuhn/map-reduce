package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.inject.Guice;

public class SocketTaskRunnerFactoryTest {
	
	@Test
	public void shouldUseExceptionConstructorForFailure() {
		SocketTaskResultFactory f = Guice.createInjector(new SocketClientConfig()).getInstance(SocketTaskResultFactory.class);
		Exception e = new Exception();
		SocketTaskResult result = f.createFailureResult(e);
		assertFalse(result.wasSuccessful());
		assertSame(e, result.getException());
		assertNull(result.getResult());
	}
	
	@Test
	public void shouldUseObjectConstructorForSuccess() {
		SocketTaskResultFactory f = Guice.createInjector(new SocketClientConfig()).getInstance(SocketTaskResultFactory.class);
		List res = new ArrayList();
		SocketTaskResult result = f.createSuccessResult(res);
		assertTrue(result.wasSuccessful());
		assertSame(res, result.getResult());
		assertNull(result.getException());
	}

}
