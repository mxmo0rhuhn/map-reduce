package ch.zhaw.mapreduce.plugins.socket;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.jmock.Sequence;
import org.jmock.auto.Auto;
import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

public abstract class AbstractMapReduceMasterSocketTest {
	

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery();
	
	@Auto
	protected Sequence events;
	
	@Mock
	protected Context ctx;
	
	@Mock
	protected ContextFactory ctxFactory;

	@Mock
	protected Persistence persistence;

	@Mock
	protected SocketAgent sAgent;

	@Mock
	protected AgentTask agentTask;
	
	@Mock
	protected WorkerTask workerTask;
	
	@Mock
	protected SocketWorkerFactory swFactory;
	
	@Mock
	protected Worker worker;

	@Mock
	protected AgentTaskFactory atFactory;
	
	@Mock
	protected SocketResultCollector resCollector;
	
	@Mock
	protected ExecutorService execMock;

	protected final String mrUuid = "mrtUuid";

	protected final String taskUuid = "taskUuid";

	protected final String input = "input";
	
	protected final List<KeyValuePair> mapResult = Arrays.asList(new KeyValuePair[] { new KeyValuePair("key1", "val1") });

	protected final List<String> reduceResult = Arrays.asList(new String[] { "res1" });


	protected final String reduceKey = "redKey";

	protected final List<KeyValuePair> reduceValues = Arrays.asList(new KeyValuePair[] {
			new KeyValuePair("ke1", "va1"), new KeyValuePair("ke2", "va2") });
}
