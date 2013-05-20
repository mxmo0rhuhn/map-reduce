package ch.zhaw.mapreduce;

import javax.inject.Provider;

import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

public class MasterTest {
	
	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private WorkerTaskFactory wtFactory;
	
	@Mock
	private Provider<Shuffler> sProvider;
	
	@Mock
	private Provider<Persistence> pProvider;
	
	@Test
	public void shouldCreateMaster() throws Exception {
		//Master m = new Master(pool, wtFactory, sProvider, pProvider, Executors.newSingleThreadScheduledExecutor(), 1L, 1, 2, 3);
	}

}
