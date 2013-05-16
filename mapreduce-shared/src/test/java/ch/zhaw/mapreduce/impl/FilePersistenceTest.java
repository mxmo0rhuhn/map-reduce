package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;

public class FilePersistenceTest {
	
	String baseDir = System.getProperty("java.io.tmpdir");
	
	private final String taskUuid = "taskUuid";
	
	private final List<KeyValuePair> mapRes = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1"), new KeyValuePair("key2", "val2")});
	
	private final List<String> redRes = Arrays.asList(new String[]{"res1", "res2"});
	
	@Test
	public void shouldReturnEmptyListForMissingMapTasks() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertNotNull(pers.getMapResults(taskUuid));
		assertTrue(pers.getMapResults(taskUuid).isEmpty());
	}
	
	@Test
	public void shouldNotBeAbleToReadResultsFromOtherInstance() {
		FilePersistence pers1 = new FilePersistence(baseDir);
		pers1.initDirectory();
		FilePersistence pers2 = new FilePersistence(baseDir);
		pers2.initDirectory();
		assertTrue(pers1.storeMapResults(taskUuid, mapRes));
		assertTrue(pers2.getMapResults(taskUuid).isEmpty());
	}
	
	@Test
	public void shouldReturnEmptyListForMissingReduceTasks() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertNotNull(pers.getReduceResults(taskUuid));
		assertTrue(pers.getReduceResults(taskUuid).isEmpty());
	}
	
	@Test
	public void shouldReadPreviouslyStoredMapValues() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		pers.storeMapResults(taskUuid, mapRes);
		assertEquals(mapRes, pers.getMapResults(taskUuid));
	}
	
	@Test
	public void shouldReadPreviouslyStoredReduceValues() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		pers.storeReduceResults(taskUuid, redRes);
		assertEquals(redRes, pers.getReduceResults(taskUuid));
	}
	
	@Test
	public void shouldNotAcceptSecondWrite() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertTrue(pers.storeReduceResults(taskUuid, redRes));
		assertFalse(pers.storeReduceResults(taskUuid, redRes));
	}
	
	@Test
	public void shouldRemoveMapResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertTrue(pers.storeMapResults(taskUuid, mapRes));
		assertTrue(pers.destroy(taskUuid));
		assertTrue(pers.getMapResults(taskUuid).isEmpty());
	}
	
	@Test
	public void shouldRemoveReduceResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertTrue(pers.storeReduceResults(taskUuid, redRes));
		assertTrue(pers.destroy(taskUuid));
		assertTrue(pers.getReduceResults(taskUuid).isEmpty());
	}
	
	@Test
	public void shouldHandleMultiMapTaskResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertTrue(pers.storeMapResults(taskUuid, mapRes));
		assertTrue(pers.storeMapResults("task2", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key3", "val3")})));
		assertTrue(pers.storeMapResults("task3", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key4", "val4")})));
		
		assertEquals(mapRes, pers.getMapResults(taskUuid));
		assertFalse(pers.getMapResults("task2").isEmpty());
		assertFalse(pers.getMapResults("task3").isEmpty());
		assertEquals(new KeyValuePair("key3", "val3"), pers.getMapResults("task2").get(0));
		assertEquals(new KeyValuePair("key4", "val4"), pers.getMapResults("task3").get(0));
	}
	
	@Test
	public void shouldHandleMultiReduceTaskResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertTrue(pers.storeReduceResults(taskUuid, redRes));
		assertTrue(pers.storeReduceResults("task2", Arrays.asList(new String[]{"res3"})));
		assertTrue(pers.storeReduceResults("task3", Arrays.asList(new String[]{"res4"})));
		
		assertEquals(redRes, pers.getReduceResults(taskUuid));
		assertFalse(pers.getReduceResults("task2").isEmpty());
		assertFalse(pers.getReduceResults("task3").isEmpty());
		assertEquals("res3", pers.getReduceResults("task2").get(0));
		assertEquals("res4", pers.getReduceResults("task3").get(0));
	}

	@Test
	public void shouldHandleMultiMapAndReduceTaskResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectory();
		assertTrue(pers.storeMapResults(taskUuid, mapRes));
		assertTrue(pers.storeReduceResults("resTask", redRes));
		assertEquals(redRes, pers.getReduceResults("resTask"));
		assertEquals(mapRes, pers.getMapResults(taskUuid));
	}

}
