package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.plugins.socket.ByteArrayClassLoader;
import ch.zhaw.mapreduce.plugins.socket.TestMapInstruction;

public class AgentTaskFactoryImplTest {

	private final String mrUuid = "mrtUuid";

	private final String taskUuid = "taskUuid";

	private final String input = "input";
	
	@Test
	public void shouldCorrectlyAssignParametersWithoutCombiner() {
		MapWorkerTask mwt = new MapWorkerTask(mrUuid, taskUuid, new TestMapInstruction(), null, input);
		MapAgentTask agentTask = (MapAgentTask) new AgentTaskFactoryImpl().createAgentTask(mwt);
		assertEquals(mrUuid, agentTask.getMapReduceTaskUuid());
		assertEquals(taskUuid, agentTask.getTaskUuid());
		assertNotNull(agentTask.getMapInstruction());
		assertEquals(TestMapInstruction.class.getName(), agentTask.getMapInstructionName());
		assertNull(agentTask.getCombinerInstruction());
		assertNull(agentTask.getCombinerInstructionName());
	}

	@Test
	public void shouldCorrectlyAssignParametersWithCombiner() {
		fail("implement me");
	}

	@Test
	public void shouldReturnCorrectName() {
		assertEquals("ch.zhaw.mapreduce.plugins.socket.TestMapInstruction",
				AgentTaskFactoryImpl.name(new TestMapInstruction()));
	}

	@Test
	public void shouldReturnCorrectByteCode() throws Exception {
		byte[] bytes = AgentTaskFactoryImpl.bytes(new TestMapInstruction());
		ByteArrayClassLoader bacl = new ByteArrayClassLoader();
		Class<?> klass = bacl.defineClass("ch.zhaw.mapreduce.plugins.socket.TestMapInstruction", bytes);
		// typischweise weurde eine exception fliegen, wenn was nicht funktionert hat
		MapInstruction mapInstr = (MapInstruction) klass.newInstance();
		assertNotNull(mapInstr);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotAcceptAnonymousClasses() throws Exception {
		AgentTaskFactoryImpl.bytes(new MapInstruction() {
			@Override
			public void map(MapEmitter emitter, String input) {
			}
		});
	}

}
