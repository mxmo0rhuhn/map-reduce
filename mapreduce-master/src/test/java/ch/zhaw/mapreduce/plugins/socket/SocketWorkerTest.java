package ch.zhaw.mapreduce.plugins.socket;

import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

public class SocketWorkerTest extends AbstractMapReduceMasterSocketTest {
	
	@Before
	public void initMockery() {
		// macht mocken von klassen moeglich. bizeli grusig, aber passt
		mockery.setImposteriser(ClassImposteriser.INSTANCE);
		mockery.setThreadingPolicy(new Synchroniser());
	}

}
