package ch.zhaw.mapreduce;

import java.util.concurrent.Executors;

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
		Pool pool = new Pool(Executors.newScheduledThreadPool(1), 1, 2, Executors.newSingleThreadScheduledExecutor(), 1000);
		pool.init();
		Master m = new Master(pool, wtFactory, sProvider, pProvider, Executors.newSingleThreadScheduledExecutor(), 1L, 1, 2, 3);
	}

}
