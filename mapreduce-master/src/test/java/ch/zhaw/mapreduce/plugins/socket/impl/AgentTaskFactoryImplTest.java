package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;
import ch.zhaw.mapreduce.plugins.socket.AbstractMapReduceMasterSocketTest;
import ch.zhaw.mapreduce.plugins.socket.ByteArrayClassLoader;
import ch.zhaw.mapreduce.plugins.socket.TestCombinerInstruction;
import ch.zhaw.mapreduce.plugins.socket.TestMapInstruction;
import ch.zhaw.mapreduce.plugins.socket.TestReduceInstruction;

public class AgentTaskFactoryImplTest extends AbstractMapReduceMasterSocketTest {

	@Test
	public void shouldCorrectlyAssignMapParametersWithoutCombiner() {
		MapWorkerTask mwt = new MapWorkerTask(taskUuid, persistence, new TestMapInstruction(), null, input);
		MapAgentTask agentTask = (MapAgentTask) new AgentTaskFactoryImpl(1).createAgentTask(mwt);
		assertEquals(taskUuid, agentTask.getTaskUuid());
		assertNotNull(agentTask.getMapInstruction());
		assertEquals(TestMapInstruction.class.getName(), agentTask.getMapInstructionName());
		assertNull(agentTask.getCombinerInstruction());
		assertNull(agentTask.getCombinerInstructionName());
	}

	@Test
	public void shouldCorrectlyAssignMapParametersWithCombiner() {
		MapWorkerTask mwt = new MapWorkerTask(taskUuid, persistence, new TestMapInstruction(),
				new TestCombinerInstruction(), input);
		MapAgentTask agentTask = (MapAgentTask) new AgentTaskFactoryImpl(2).createAgentTask(mwt);
		assertEquals(taskUuid, agentTask.getTaskUuid());
		assertNotNull(agentTask.getMapInstruction());
		assertEquals(TestMapInstruction.class.getName(), agentTask.getMapInstructionName());
		assertNotNull(agentTask.getCombinerInstruction());
		assertEquals(TestCombinerInstruction.class.getName(), agentTask.getCombinerInstructionName());
	}

	@Test
	public void shouldCorrectlyAssignReduceParameters() {
		ReduceWorkerTask rwt = new ReduceWorkerTask(taskUuid, persistence, new TestReduceInstruction(), reduceKey,
				reduceValues);
		ReduceAgentTask agentTask = (ReduceAgentTask) new AgentTaskFactoryImpl(3).createAgentTask(rwt);
		assertEquals(taskUuid, agentTask.getTaskUuid());
		assertEquals(TestReduceInstruction.class.getName(), agentTask.getReduceInstructionName());
		assertNotNull(agentTask.getReduceInstruction());
	}

	@Test
	public void shouldReturnCorrectName() {
		assertEquals("ch.zhaw.mapreduce.plugins.socket.TestMapInstruction",
				AgentTaskFactoryImpl.name(new TestMapInstruction()));
	}

	@Test
	public void shouldReturnCorrectByteCode() throws Exception {
		byte[] bytes = new AgentTaskFactoryImpl(5).bytes(new TestMapInstruction());
		ByteArrayClassLoader bacl = new ByteArrayClassLoader();
		Class<?> klass = bacl.defineClass("ch.zhaw.mapreduce.plugins.socket.TestMapInstruction", bytes);
		// typischweise weurde eine exception fliegen, wenn was nicht funktionert hat
		MapInstruction mapInstr = (MapInstruction) klass.newInstance();
		assertNotNull(mapInstr);
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotAcceptAnonymousClasses() throws Exception {
		new AgentTaskFactoryImpl(1).bytes(new MapInstruction() {
			@Override
			public void map(MapEmitter emitter, String input) {
			}
		});
	}

}
