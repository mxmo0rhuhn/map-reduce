package ch.zhaw.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.inject.Provider;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.WorkerTask.State;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

public class MasterTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery( ){{ setThreadingPolicy(new Synchroniser()); }};

	@Mock
	private MapInstruction mInstr;

	@Mock
	private CombinerInstruction cInstr;

	@Mock
	private ReduceInstruction rInstr;

	@Mock
	private WorkerTaskFactory wtFactory;

	@Mock
	private Shuffler shuffler;

	@Mock
	private Provider<Persistence> pProvider;

	@Mock
	private Persistence pers;

	@Mock
	private Pool pool;

	private final String mapin = "mapin";

	private final String redkey1 = "redkey1";

	private final List<KeyValuePair> redval1 = new ArrayList<KeyValuePair>();

	private final String redkey2 = "redkey2";

	private final List<KeyValuePair> redval2 = new ArrayList<KeyValuePair>();

	private final String taskUuid = "taskUuid";

	@Test
	public void shouldRemoveDoneTasksAndRestartFailedMap() throws Exception {
		Master m = new Master(pool, wtFactory, shuffler, pProvider, 10000);
		List<MapWorkerTask> tasks = l(mtask(State.COMPLETED), mtask(State.ENQUEUED), mtask(State.FAILED), mtask(),
				mtask(State.INPROGRESS));
		mockery.checking(new Expectations() {
			{
				oneOf(wtFactory).createMapWorkerTask(mInstr, cInstr, mapin, pers);
				will(returnValue(mtask()));
				oneOf(pool).enqueueTask(with(aNonNull(MapWorkerTask.class)));
			}
		});
		assertFalse(m.housekeepingMap(tasks));
		assertEquals(4, tasks.size());
	}

	@Test
	public void shouldRemoveDoneTasksAndRestartFailedReduce() throws Exception {
		Master m = new Master(pool, wtFactory, shuffler, pProvider, 10000);
		List<ReduceWorkerTask> tasks = l(rtask(State.COMPLETED), rtask(State.ENQUEUED), rtask(State.FAILED), rtask(),
				rtask(State.INPROGRESS));
		mockery.checking(new Expectations() {
			{
				oneOf(wtFactory).createReduceWorkerTask(rInstr, redkey1, redval1, pers);
				will(returnValue(rtask()));
				oneOf(pool).enqueueTask(with(aNonNull(ReduceWorkerTask.class)));
			}
		});
		assertFalse(m.housekeepingReduce(tasks));
		assertEquals(4, tasks.size());
	}

	@Test
	public void shouldRunMapTasks() throws InterruptedException {
		// die tasks werden alle als completed aufgegeben, dann werden sie grad wieder entfernt
		Master m = new Master(pool, wtFactory, shuffler, pProvider, 10000);
		mockery.checking(new Expectations() {
			{
				oneOf(wtFactory).createMapWorkerTask(mInstr, cInstr, "a", pers);
				will(returnValue(mtask(State.COMPLETED)));
				oneOf(wtFactory).createMapWorkerTask(mInstr, cInstr, "b", pers);
				will(returnValue(mtask(State.COMPLETED)));
				oneOf(wtFactory).createMapWorkerTask(mInstr, cInstr, "c", pers);
				will(returnValue(mtask(State.COMPLETED)));
				exactly(3).of(pool).enqueueTask(with(aNonNull(MapWorkerTask.class)));
			}
		});
		m.runMapTasks(mInstr, cInstr, i("a", "b", "c"), pers);
	}

	@Test
	public void shouldRunReduceTasks() throws InterruptedException {
		// die tasks werden alle als completed aufgegeben, dann werden sie grad wieder entfernt
		Master m = new Master(pool, wtFactory, shuffler, pProvider, 10000);
		mockery.checking(new Expectations() {
			{
				oneOf(wtFactory).createReduceWorkerTask(rInstr, redkey1, redval1, pers);
				will(returnValue(rtask(State.COMPLETED)));
				oneOf(wtFactory).createReduceWorkerTask(rInstr, redkey2, redval2, pers);
				will(returnValue(rtask(State.COMPLETED)));
				exactly(2).of(pool).enqueueTask(with(aNonNull(ReduceWorkerTask.class)));
			}
		});
		Map<String, List<KeyValuePair>> shuffled = new TreeMap<String, List<KeyValuePair>>();
		shuffled.put(redkey1, redval1);
		shuffled.put(redkey2, redval2);
		m.runReduceTasks(rInstr, shuffled, pers);
	}

	@Test
	public void shouldOnlyRunCertainAmountOfMapTasks() throws InterruptedException {
		final Master m = new Master(pool, wtFactory, shuffler, pProvider, 2);
		mockery.checking(new Expectations() {
			{
				oneOf(wtFactory).createMapWorkerTask(mInstr, cInstr, "a", pers); will(returnValue(mtask()));
				oneOf(wtFactory).createMapWorkerTask(mInstr, cInstr, "b", pers); will(returnValue(mtask()));
				exactly(2).of(pool).enqueueTask(with(aNonNull(MapWorkerTask.class)));
			}
		});
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					m.runMapTasks(mInstr, cInstr, i("a", "b", "c"), pers);
				} catch (InterruptedException e) {
					// stopped
				}
			}
		});

		t.start();
		Thread.yield();
		t.join(300);
		t.interrupt();
	}
	
	@Test
	public void shouldOnlyRunCertainAmountOfReduceTasks() throws InterruptedException {
		final Master m = new Master(pool, wtFactory, shuffler, pProvider, 1);
		mockery.checking(new Expectations() {
			{
				oneOf(wtFactory).createReduceWorkerTask(rInstr, redkey1, redval1, pers); will(returnValue(rtask()));
				exactly(1).of(pool).enqueueTask(with(aNonNull(ReduceWorkerTask.class)));
			}
		});
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					Map<String, List<KeyValuePair>> shuffled = new TreeMap<String, List<KeyValuePair>>();
					shuffled.put(redkey1, redval1);
					shuffled.put(redkey2, redval2);
					m.runReduceTasks(rInstr, shuffled, pers);
				} catch (InterruptedException e) {
					// stopped
				}
			}
		});

		t.start();
		Thread.yield();
		t.join(300);
		t.interrupt();
	}

	Iterator<String> i(String... vals) {
		return Arrays.asList(vals).iterator();
	}

	List<ReduceWorkerTask> l(ReduceWorkerTask... tasks) {
		return new ArrayList<ReduceWorkerTask>(Arrays.asList(tasks));
	}

	ReduceWorkerTask rtask() {
		ReduceWorkerTask mwt = new ReduceWorkerTask(taskUuid, pers, rInstr, redkey1, redval1);
		return mwt;
	}

	ReduceWorkerTask rtask(WorkerTask.State state) {
		ReduceWorkerTask mwt = rtask();
		mwt.setState(state);
		return mwt;
	}

	List<MapWorkerTask> l(MapWorkerTask... tasks) {
		return new ArrayList<MapWorkerTask>(Arrays.asList(tasks));
	}

	MapWorkerTask mtask() {
		MapWorkerTask mwt = new MapWorkerTask(taskUuid, pers, mInstr, cInstr, mapin);
		return mwt;
	}

	MapWorkerTask mtask(WorkerTask.State state) {
		MapWorkerTask mwt = mtask();
		mwt.setState(state);
		return mwt;
	}

}
