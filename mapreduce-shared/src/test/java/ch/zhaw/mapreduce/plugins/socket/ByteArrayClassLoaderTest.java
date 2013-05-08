package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class ByteArrayClassLoaderTest {
	
	@Test
	public void shouldLoadRegularClass() throws Exception {
		ByteArrayClassLoader loader = new ByteArrayClassLoader();
		Class<?> klass = loader.defineClass(TestClassLoaderClassA.class.getName(), bytes(new TestClassLoaderClassA()));
		assertNotNull(klass.newInstance());
	}
	
	@Test
	public void shouldHandleLoadingTheSameClassTwice() throws Exception {
		ByteArrayClassLoader loader = new ByteArrayClassLoader();
		Class<?> klass1 = loader.defineClass(TestClassLoaderClassA.class.getName(), bytes(new TestClassLoaderClassA()));
		Class<?> klass2 = loader.defineClass(TestClassLoaderClassA.class.getName(), bytes(new TestClassLoaderClassA()));
		assertNotNull(klass1.newInstance());
		assertNotNull(klass2.newInstance());
	}
	
	@Test
	public void shouldNotMixUpClasses() throws Exception {
		ByteArrayClassLoader loader = new ByteArrayClassLoader();
		Class<?> klass1 = loader.defineClass(TestClassLoaderClassA.class.getName(), bytes(new TestClassLoaderClassA()));
		Class<?> klass2 = loader.defineClass(TestClassLoaderClassB.class.getName(), bytes(new TestClassLoaderClassB()));
		assertNotSame(klass1, klass2);
	}
	
	/** kopiert fuer tests von: AgentTaskFactoryImpl */
	static byte[] bytes(Object instance) {
		Class<?> klass = instance.getClass();
		if (klass.isAnonymousClass() || klass.isLocalClass() || klass.isMemberClass()) {
			throw new IllegalArgumentException("Only regular Top-Level Classes are allowed for now");
		}
		String resourceName = klass.getName().replace('.', '/') + ".class";
		InputStream is = klass.getClassLoader().getResourceAsStream(resourceName);
		if (is == null) {
			throw new IllegalArgumentException("ResouceNotFound: " + resourceName);
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buf = new byte[256];
		int read;
		try {
			while ((read = is.read(buf)) != -1) {
				bos.write(buf, 0, read);
			}
		} catch (Exception e) {
			// das ist nicht kopiert. im original loggen wir hier!!
			throw new IllegalArgumentException(e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException ignored) {
				}
			}
		}
		return bos.toByteArray();
	}

}
