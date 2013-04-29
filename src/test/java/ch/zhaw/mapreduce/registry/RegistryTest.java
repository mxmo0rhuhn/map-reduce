package ch.zhaw.mapreduce.registry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Master;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.WorkerTaskFactory;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.matcher.Matchers;

public class RegistryTest {

	@Rule 
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Mock
	private MapInstruction mapInstr;
	
	@Mock
	private CombinerInstruction combInstr;
	
	@Mock
	private ReduceInstruction reduceInstr;

	private String input = "input";

	private List<KeyValuePair> toDo = Collections.emptyList();

	@Test
	public void shouldDefineBindingForMaster() {
		assertNotNull(Guice.createInjector(new MapReduceConfig()).getInstance(Master.class));
	}

	@Test
	public void poolShouldBeSingleton() {
		Injector injector = Guice.createInjector(new MapReduceConfig());
		Pool p1 = injector.getInstance(Pool.class);
		Pool p2 = injector.getInstance(Pool.class);
		assertSame(p1, p2);
	}

	@Test
	public void poolShouldBeSingletonInDifferentThreads() throws InterruptedException {
		final Injector injector = Guice.createInjector(new MapReduceConfig());
		final Pool[] pools = new Pool[2];
		Thread t1 = new Thread(new Runnable() {
			@Override
			public void run() {
				pools[0] = injector.getInstance(Pool.class);
			}
		});

		Thread t2 = new Thread(new Runnable() {
			@Override
			public void run() {
				pools[1] = injector.getInstance(Pool.class);
			}
		});

		t1.start();
		t2.start();
		t1.join();
		t2.join();
		assertSame(pools[0], pools[1]);
		assertSame(pools[1], injector.getInstance(Pool.class));
	}

	@Test
	public void shouldInvokeInitOnLocalThreadPool() {
		// hier wird explizit ein neuer injector kreiert, da der pool im kontext von guice ein singleton ist und wir
		// sonst nicht sicherstellen koennen, dass hier das erste mal eine instanz vom pool angefordert wird. dies ist
		// aber notwendig, da die init method nur beim ersten mal aufgerufen wird.
		Pool p = Guice.createInjector(new AbstractModule() {

			@Override
			protected void configure() {
				bind(Pool.class);
				bindListener(Matchers.any(), new PostConstructFeature());
			}

			@Provides
			@PoolExecutor
			public Executor poolExec() {
				return Executors.newSingleThreadExecutor();
			}

		}).getInstance(Pool.class);
		assertTrue(p.isRunning());
	}

	@Test
	public void shouldSetMapAndCombinerTaskToMapRunner() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mapWorkerTask = factory.createMapWorkerTask("uuid", mapInstr, combInstr, input);
		assertSame(mapInstr, mapWorkerTask.getMapInstruction());
		assertSame(combInstr, mapWorkerTask.getCombinerInstruction());
	}

	@Test
	public void shouldCopeWithNullCombinerTask() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mapperTask = factory.createMapWorkerTask("uuid", mapInstr, null, input);
		assertNotNull(mapperTask);
	}

	@Test
	public void shouldCreatePrototypesForMapRunners() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		assertNotSame(factory.createMapWorkerTask("uuid1", mapInstr, combInstr, input),
				factory.createMapWorkerTask("uuid2", mapInstr, combInstr, input));
	}

	@Test
	public void shouldSetReduceTaskToReduceRunner() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		ReduceWorkerTask reduceRunner = factory.createReduceWorkerTask("uuid", "key", reduceInstr, this.toDo);
		assertSame(reduceInstr, reduceRunner.getReduceTask());
	}

	@Test
	public void shouldCreatePrototypesForReduceRunners() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		assertNotSame(factory.createReduceWorkerTask("uuid1", "key1", reduceInstr, toDo),
				factory.createReduceWorkerTask("uuid2", "key2", reduceInstr, toDo));
	}

	@Test
	public void shouldSetMapReduceUUIDToMapTask() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mwt = factory.createMapWorkerTask("uuid", mapInstr, null, input);
		assertEquals("uuid", mwt.getMapReduceTaskUUID());
	}

	@Test
	public void shouldSetWorkerTaskUUIDToMapTask() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mwt = factory.createMapWorkerTask("uuid", mapInstr, null, input);
		assertNotNull(mwt.getUUID());
	}

	@Test
	public void shouldGenerateDistinctWorkerTaskUUIDs() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mwt1 = factory.createMapWorkerTask("uuid", mapInstr, null, input);
		MapWorkerTask mwt2 = factory.createMapWorkerTask("uuid", mapInstr, null, input);
		String uuid1 = mwt1.getUUID();
		String uuid2 = mwt2.getUUID();
		assertNotNull(uuid1);
		assertNotNull(uuid2);
		assertFalse(uuid1.equals(uuid2));
	}

	@Test
	public void shouldSetUUIDToReduceTask() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		ReduceWorkerTask mwt = factory.createReduceWorkerTask("uuid", "key",
				reduceInstr, toDo);
		assertEquals("uuid", mwt.getMapReduceTaskUUID());
	}

	@Test
	public void shouldProvideDifferentUUIDEveryTime() {
		Injector injector = Guice.createInjector(new MapReduceConfig());
		Master m1 = injector.getInstance(Master.class);
		Master m2 = injector.getInstance(Master.class);
		assertFalse(m1.getMapReduceTaskUUID().equals(m2.getMapReduceTaskUUID()));
	}
}
