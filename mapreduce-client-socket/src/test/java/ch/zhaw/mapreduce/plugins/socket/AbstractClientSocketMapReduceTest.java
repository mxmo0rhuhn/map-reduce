package ch.zhaw.mapreduce.plugins.socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.plugins.socket.impl.TestCombinerInstruction;
import ch.zhaw.mapreduce.plugins.socket.impl.TestMapInstruction;
import ch.zhaw.mapreduce.plugins.socket.impl.TestReduceInstruction;

/**
 * Dies ist eine abstrakte Test-HIlfsklasse und definiert alle Mocks und Konstanten, um boilerplate zu vermeiden.
 * Unit-Tests koennen von dieser ableiten.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public abstract class AbstractClientSocketMapReduceTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();

	@Mock
	protected MapInstruction mapInstr;

	@Mock
	protected CombinerInstruction combInstr;

	@Mock
	protected ReduceInstruction redInstr;

	@Mock
	protected WorkerTask task;

	@Mock
	protected Context ctx;

	@Mock
	protected ContextFactory ctxFactory;

	@Mock
	protected AgentTask aTask;

	@Mock
	protected SocketTaskResult result;

	@Mock
	protected TaskRunnerFactory trFactory;
	
	@Mock
	protected MapTaskRunnerFactory mtrFactory;

	@Mock
	protected ReduceTaskRunnerFactory rtrFactory;

	@Mock
	protected TaskRunner taskRunner;

	@Mock
	protected SocketTaskResultFactory strFactory;

	@Mock
	protected SocketTaskResult stResult;
	
	protected final String miName = TestMapInstruction.class.getName();

	protected final byte[] mi = bytes(new TestMapInstruction());

	protected final String ciName = TestCombinerInstruction.class.getName();

	protected final byte[] ci = bytes(new TestCombinerInstruction());

	protected final String riName = TestReduceInstruction.class.getName();

	protected final byte[] ri = bytes(new TestReduceInstruction());

	protected final String clientIp = "123.234.124.234";

	protected List<KeyValuePair> mapResult = Arrays.asList(new KeyValuePair("key1", "val1"));

	protected final String mrtUuid = "mrtUuid";

	protected final String taskUuid = "taskUuid";

	protected final String mapInput = "mapInput";
	
	protected final String reduceKey = "redKey";
	
	protected final List<KeyValuePair> reduceValues = Arrays.asList(new KeyValuePair("key1", "val1"));
	
	/** Kopiert fuer tests von: SocketTaskFactoryImpl */
	private static byte[] bytes(Object instance) {
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
