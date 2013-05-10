package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;

public class InMemoryPersistenceTest {
	
	private final String mrUuid = "mrtUuid";
	
	private final String taskUuid = "taskUuid";
	
	@Test
	public void shouldReadPreviouslyStoredMapValues() {
		InMemoryPersistence pers = new InMemoryPersistence();
		pers.storeMap(mrUuid, taskUuid, "key", "val");
		assertTrue(pers.getMap(mrUuid, taskUuid).size() == 1);
		assertEquals("key", pers.getMap(mrUuid, taskUuid).get(0).getKey());
		assertEquals("val", pers.getMap(mrUuid, taskUuid).get(0).getValue());
		
		// nicht vermischen
		assertTrue(pers.getReduce(mrUuid, taskUuid).size() == 0);
	}
	
	@Test
	public void shouldReadPreviouslyStoredReduceValues() {
		InMemoryPersistence pers = new InMemoryPersistence();
		pers.storeReduce(mrUuid, taskUuid, "result");
		assertTrue(pers.getReduce(mrUuid, taskUuid).size() == 1);
		assertEquals("result", pers.getReduce(mrUuid, taskUuid).get(0));
		
		// nicht vermischen
		assertTrue(pers.getMap(mrUuid, taskUuid).size() == 0);
	}
	
	@Test
	public void shouldReturnEmptyCollectionForWrongIds() {
		InMemoryPersistence pers = new InMemoryPersistence();
		assertNotNull(pers.getMap("inexistend", "asdFooBar"));
		assertNotNull(pers.getReduce("inexistend", "asdFooBar"));
	}
	
	@Test
	public void shouldCombineWithExistingMapResults() {
		InMemoryPersistence pers = new InMemoryPersistence();
		pers.storeMap(mrUuid, taskUuid, "key1", "val1");
		pers.storeMap(mrUuid, taskUuid, "key2", "val2");
		assertEquals(2, pers.getMap(mrUuid, taskUuid).size());
		assertTrue(pers.getMap(mrUuid, taskUuid).contains(new KeyValuePair("key1", "val1")));
		assertTrue(pers.getMap(mrUuid, taskUuid).contains(new KeyValuePair("key2", "val2")));
	}
	
	@Test
	public void shouldCombineWithExistingReduceResults() {
		InMemoryPersistence pers = new InMemoryPersistence();
		pers.storeReduce(mrUuid, taskUuid, "res1");
		pers.storeReduce(mrUuid, taskUuid, "res2");
		assertEquals(2, pers.getReduce(mrUuid, taskUuid).size());
		assertTrue(pers.getReduce(mrUuid, taskUuid).contains("res1"));
		assertTrue(pers.getReduce(mrUuid, taskUuid).contains("res2"));
	}
	
	@Test
	public void shouldRemoveMapResults() {
		InMemoryPersistence pers = new InMemoryPersistence();
		pers.storeMap(mrUuid, taskUuid, "key", "val");
		pers.destroy(mrUuid, taskUuid);
		assertTrue(pers.getReduce(mrUuid, taskUuid).isEmpty());
	}
	
	@Test
	public void shouldRemoveReduceResults() {
		InMemoryPersistence pers = new InMemoryPersistence();
		pers.storeReduce(mrUuid, taskUuid, "res1");
		pers.destroy(mrUuid, taskUuid);
		assertTrue(pers.getReduce(mrUuid, taskUuid).isEmpty());
	}

}
