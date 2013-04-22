package ch.zhaw.mapreduce;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.DeterministicExecutor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.zhaw.mapreduce.roundtriptest.WordsInputSplitter;

import com.google.inject.Provider;

@RunWith(JMock.class)
public class MasterTest {

	private Mockery context;
	private WorkerTaskFactory factory;
	private Provider<Shuffler> shuffleProvider;
	private MapInstruction mapInstruction;
	private ReduceInstruction reduceInstruction;
	private CombinerInstruction combinerInstruction;
	private Logger dummyOut;
	
	private static <T> Provider<T> toProvider(final T instance) {
		return new Provider<T>() {
			@Override
			public T get() {
				return instance;
			}
		};
	}

	@Before
	public void initMock() {
		this.context = new JUnit4Mockery();
		this.factory = this.context.mock(WorkerTaskFactory.class);
		this.mapInstruction = this.context.mock(MapInstruction.class);
		this.reduceInstruction = this.context.mock(ReduceInstruction.class);
		this.combinerInstruction = this.context.mock(CombinerInstruction.class);
		this.shuffleProvider = toProvider(this.context.mock(Shuffler.class));
		this.dummyOut = Logger.getLogger(MasterTest.class.getName());
	}

	@Test
	public void shouldSetUUId() {
		Master m = new Master(null, factory, "uuid", shuffleProvider);
		assertEquals("uuid", m.getMapReduceTaskUUID());
	}

	@Test
	public void shouldCreateMapTasks() {
		DeterministicExecutor exec = new DeterministicExecutor();
		Master m = new Master(new Pool(exec, dummyOut), factory, "uuid", shuffleProvider);
		final Sequence seq = this.context.sequence("seq");
		this.context.checking(new Expectations() {
			{
				oneOf(factory).createMapWorkerTask(with("uuid"), with(aNonNull(String.class)),
						with(mapInstruction), with(combinerInstruction), with("foo"));
				inSequence(seq);
				oneOf(factory).createMapWorkerTask(with("uuid"), with(aNonNull(String.class)), with(mapInstruction),
						with(combinerInstruction), with("bar"));
			}
		});
		Set<KeyValuePair<String, WorkerTask>> activeTasks = new LinkedHashSet<KeyValuePair<String, WorkerTask>>();
		
		Map<String, KeyValuePair> tasks = m.runMap(mapInstruction, combinerInstruction,
				new WordsInputSplitter("foo bar", 1), activeTasks);
		assertEquals("should have create two map tasks", 2, tasks.size());
	}

	@Test
	public void shouldCreateShuffleTasks() {
		Master m = new Master(pool, factory, "uuid");
		final Worker worker1 = this.context.mock(Worker.class, "worker1");
		final Worker worker2 = this.context.mock(Worker.class, "worker2");
		Collection<Worker> mapWorkers = new ArrayList<Worker>() {
			private static final long serialVersionUID = 1L;
			{
				add(worker1);
				add(worker2);
			}
		};
		final Sequence seq = this.context.sequence("seq");
		this.context.checking(new Expectations() {
			{
				oneOf(worker1).getMapResults("uuid");
				inSequence(seq);
				oneOf(worker2).getMapResults("uuid");
			}
		});
		Map<String, List<KeyValuePair>> shuffled = m.runShuffle(mapWorkers);
		assertEquals(0, shuffled.size());
	}

	@Test
	public void shouldPutSameKeysInSameList() {
		Master m = new Master(pool, factory, "uuid");
		Map<String, List<KeyValuePair>> pairs = new HashMap<String, List<KeyValuePair>>();
		pairs.put("uuid", new ArrayList<KeyValuePair>() {
			{
				add(new KeyValuePair("key1", "value1"));
				add(new KeyValuePair("key1", "value2"));
			}
		});
		final Worker worker1 = new MockWorker(pairs);
		Collection<Worker> mapWorkers = new ArrayList<Worker>() {
			{
				add(worker1);
			}
		};
		Map<String, List<KeyValuePair>> shuffled = m.runShuffle(mapWorkers);
		assertEquals(1, shuffled.size());
		assertEquals(2, shuffled.get("key1").size());
	}

	@Test
	public void shouldCreateReduceTasks() {
		Master m = new Master(pool, factory, "uuid");
		Map<String, List<KeyValuePair>> reduceInputs = new HashMap<String, List<KeyValuePair>>();
		reduceInputs.put("key1", new LinkedList<KeyValuePair>() {
			{
				add(new KeyValuePair("key1", "value1"));
			}
		});
		reduceInputs.put("key2", new LinkedList<KeyValuePair>() {
			{
				add(new KeyValuePair("key2", "value2"));
			}
		});
		final Sequence seq = this.context.sequence("seq");
		this.context.checking(new Expectations() {
			{
				oneOf(factory).createReduceWorkerTask(with("uuid"), with("key1"),
						with(reduceInstruction), with(aNonNull(List.class)));
				inSequence(seq);
				oneOf(factory).createReduceWorkerTask(with("uuid"), with("key2"),
						with(reduceInstruction), with(aNonNull(List.class)));
			}
		});
		Set<WorkerTask> reduceTasks = m.runReduce(reduceInstruction, reduceInputs);
		assertEquals(2, reduceTasks.size());
	}
}

class MockWorker implements Worker {

	Map<String, List<KeyValuePair>> pairs;

	MockWorker(Map<String, List<KeyValuePair>> pairs) {
		this.pairs = pairs;
	}

	@Override
	public void executeTask(WorkerTask task) {
	}

	@Override
	public void storeMapResult(String mapReduceTaskUID, KeyValuePair pair) {
		if (!this.pairs.containsKey(mapReduceTaskUID)) {
			this.pairs.put(mapReduceTaskUID, new LinkedList<KeyValuePair>());
		}
		this.pairs.get(mapReduceTaskUID).add(pair);
	}

	@Override
	public void storeReduceResult(String mapReduceTaskUID, KeyValuePair pair) {
	}

	@Override
	public List<KeyValuePair> getReduceResults(String mapReduceTaskUID) {
	}

	@Override
	public List<KeyValuePair> getMapResults(String mapReduceTaskUID) {
		return this.pairs.get(mapReduceTaskUID);
	}

	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		this.pairs.clear();
	}

	@Override
	public void replaceMapResult(String mapReduceTaskUID, List<KeyValuePair> newResult) {
		this.pairs.put(mapReduceTaskUID, newResult);
	}

	@Override
	public List<String> getReduceResult(String mapReduceTaskUID, String inputUID) {
		return this.pairs.get(mapReduceTaskUID);
	}

	@Override
	public List<KeyValuePair<String, String>> getMapResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		return null;
	}

}
