package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;

public class ReduceAgentTaskTest {
	
	private final String taskUuid = "taskUuid";
	
	private final String riName = "ri";
	
	private final byte[] ri = new byte[]{1,2, 3};
	
	private final String key = "key";
	
	private final List<KeyValuePair> vals = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key", "val")});
	
	@Test
	public void shouldCorrectlyAssignParameters() {
		ReduceAgentTask mat = new ReduceAgentTask(taskUuid, riName, ri, key, vals);
		assertEquals(taskUuid, mat.getTaskUuid());
		assertEquals(riName, mat.getReduceInstructionName());
		assertEquals(ri, mat.getReduceInstruction());
		assertEquals(key, mat.getKey());
		assertEquals(vals, mat.getValues());
	}
}
