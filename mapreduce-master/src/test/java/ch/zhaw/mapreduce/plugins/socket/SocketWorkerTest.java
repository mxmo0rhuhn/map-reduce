package ch.zhaw.mapreduce.plugins.socket;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.jmock.auto.Mock;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Rule;
import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;

public class SocketWorkerTest {

	@Rule
	public JUnitRuleMockery mockery = new JUnitRuleMockery() {
		{
			// macht mocken von klassen moeglich. bizeli grusig, aber passt
			setImposteriser(ClassImposteriser.INSTANCE);
			setThreadingPolicy(new Synchroniser());
		}
	};

	@Mock
	private Persistence persistence;

	@Mock
	private SocketAgent agent;

	@Mock
	private AgentTask agentTask;

	@Mock
	private AgentTaskFactory atFactory;

	private final String mrUuid = "mapreduceuuid";

	private final String taskUuid = "taskuuid";

	private final List<KeyValuePair> mapResult = Arrays.asList(new KeyValuePair[] { new KeyValuePair("key1", "val1") });

	private final List<String> reduceResult = Arrays.asList(new String[] { "res1" });

	@Test
	public void shouldRunReduceTaskOnAgent() throws Exception {
		fail("implement me");
	}

	@Test
	public void shouldRunMapTaskOnAgent() throws Exception {
		fail("implement me");
	}

	@Test
	public void shouldGoBackToPoolOnSuccess() throws Exception {
		fail("implement me");
	}

	@Test
	public void shouldGoBackToPoolOnFailure() throws Exception {
		fail("implement me");
	}


}
