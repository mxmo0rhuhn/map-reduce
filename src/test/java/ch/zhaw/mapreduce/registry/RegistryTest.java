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

import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

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

@RunWith(JMock.class)
public class RegistryTest {

	private Mockery context;

	private String input;

	private List<KeyValuePair> toDo;

	@Before
	public void initMock() {
		this.context = new JUnit4Mockery();
		this.input = "input";
		this.toDo = Collections.emptyList();
	}

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
		MapInstruction mapTask = this.context.mock(MapInstruction.class);
		CombinerInstruction combinerTask = this.context.mock(CombinerInstruction.class);
		MapWorkerTask mapWorkerTask = factory.createMapWorkerTask("uuid", mapTask, combinerTask, input);
		assertSame(mapTask, mapWorkerTask.getMapInstruction());
		assertSame(combinerTask, mapWorkerTask.getCombinerInstruction());
	}

	@Test
	public void shouldCopeWithNullCombinerTask() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapInstruction mapTask = this.context.mock(MapInstruction.class);
		MapWorkerTask mapperTask = factory.createMapWorkerTask("uuid", mapTask, null, input);
		assertNotNull(mapperTask);
	}

	@Test
	public void shouldCreatePrototypesForMapRunners() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapInstruction mapTask = this.context.mock(MapInstruction.class);
		CombinerInstruction combinerTask = this.context.mock(CombinerInstruction.class);
		assertNotSame(factory.createMapWorkerTask("uuid1", mapTask, combinerTask, input),
				factory.createMapWorkerTask("uuid2", mapTask, combinerTask, input));
	}

	@Test
	public void shouldSetReduceTaskToReduceRunner() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		ReduceInstruction reduceTask = this.context.mock(ReduceInstruction.class);
		ReduceWorkerTask reduceRunner = factory.createReduceWorkerTask("uuid", "key", reduceTask, this.toDo);
		assertSame(reduceTask, reduceRunner.getReduceTask());
	}

	@Test
	public void shouldCreatePrototypesForReduceRunners() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		ReduceInstruction reduceTask = this.context.mock(ReduceInstruction.class);
		assertNotSame(factory.createReduceWorkerTask("uuid1", "key1", reduceTask, toDo),
				factory.createReduceWorkerTask("uuid2", "key2", reduceTask, toDo));
	}

	@Test
	public void shouldSetMapReduceUUIDToMapTask() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mwt = factory.createMapWorkerTask("uuid", this.context.mock(MapInstruction.class), null, input);
		assertEquals("uuid", mwt.getMapReduceTaskUUID());
	}

	@Test
	public void shouldSetWorkerTaskUUIDToMapTask() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mwt = factory.createMapWorkerTask("uuid", this.context.mock(MapInstruction.class), null, input);
		assertNotNull(mwt.getUUID());
	}

	@Test
	public void shouldGenerateDistinctWorkerTaskUUIDs() {
		WorkerTaskFactory factory = Guice.createInjector(new MapReduceConfig()).getInstance(WorkerTaskFactory.class);
		MapWorkerTask mwt1 = factory.createMapWorkerTask("uuid", this.context.mock(MapInstruction.class, "mi1"), null, input);
		MapWorkerTask mwt2 = factory.createMapWorkerTask("uuid", this.context.mock(MapInstruction.class, "mi2"), null, input);
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
				this.context.mock(ReduceInstruction.class), toDo);
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
