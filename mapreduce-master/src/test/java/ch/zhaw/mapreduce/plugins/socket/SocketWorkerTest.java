package ch.zhaw.mapreduce.plugins.socket;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.ExactCommandExecutor;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

public class SocketWorkerTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery() {
		{
			// macht mocken von klassen moeglich. bizeli hacky
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};

	@Mock
	private Persistence persistence;

	@Mock
	private ClientCallback callback;

	@Mock
	private MapWorkerTask mapTask;
	
	@Mock
	private ReduceWorkerTask reduceTask;

	private final String mrUuid = "mapreduceuuid";

	private final String taskUuid = "taskuuid";
	
	private final List<KeyValuePair> mapResult = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1")});
	
	private final List<String> reduceResult = Arrays.asList(new String[] {"res1"});

	@Test
	public void shouldRunMapTaskOnCallback() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		SocketWorker worker = new SocketWorker(callback, exec, persistence, p);
		this.mockery.checking(new Expectations() {
			{
				atLeast(1).of(mapTask).getMapReduceTaskUUID();
				will(returnValue(mrUuid));
				atLeast(1).of(mapTask).getUUID();
				will(returnValue(taskUuid));
				oneOf(callback).runTask(mapTask);
				will(returnValue(mapResult));
				oneOf(persistence).storeMap(mrUuid, taskUuid, "key1", "val1");
			}
		});
		worker.executeTask(mapTask);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
	}
	
	@Test
	public void shouldRunReduceTaskOnCallback() {
		ExactCommandExecutor exec = new ExactCommandExecutor(1);
		Pool p = new Pool(Executors.newSingleThreadExecutor());
		p.init();
		SocketWorker worker = new SocketWorker(callback, exec, persistence, p);
		this.mockery.checking(new Expectations() {
			{
				atLeast(1).of(reduceTask).getMapReduceTaskUUID();
				will(returnValue(mrUuid));
				atLeast(1).of(reduceTask).getUUID();
				will(returnValue(taskUuid));
				oneOf(callback).runTask(reduceTask);
				will(returnValue(reduceResult));
				oneOf(persistence).storeReduce(mrUuid, taskUuid, "res1");
			}
		});
		worker.executeTask(reduceTask);
		exec.waitForExpectedTasks(200, TimeUnit.MILLISECONDS);
	}

}
