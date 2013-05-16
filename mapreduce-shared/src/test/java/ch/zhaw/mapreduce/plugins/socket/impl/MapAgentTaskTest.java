package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MapAgentTaskTest {
	
	private final String taskUuid = "taskUuid";
	
	private final String miName = "mi";
	
	private final byte[] mi = new byte[]{1,2, 3};
	
	private final String ciName = "ci";
	
	private final byte[] ci = new byte[]{2, 3, 4};
	
	private final String input = "input";
	
	@Test
	public void shouldCorrectlyAssignParameters() {
		MapAgentTask mat = new MapAgentTask(taskUuid, miName, mi, ciName, ci, input);
		assertEquals(taskUuid, mat.getTaskUuid());
		assertEquals(miName, mat.getMapInstructionName());
		assertEquals(mi, mat.getMapInstruction());
		assertEquals(ciName, mat.getCombinerInstructionName());
		assertEquals(ci, mat.getCombinerInstruction());
		assertEquals(input, mat.getInput());
	}
}
