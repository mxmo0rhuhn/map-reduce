package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;

public class FilePersistenceTest {
	
	private final String baseDir = System.getProperty("java.io.tmpdir");
	
	private final String taskUuid = "taskUuid";
	
	private final List<KeyValuePair> mapRes = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1"), new KeyValuePair("key2", "val2")});
	
	private final List<String> redRes = Arrays.asList(new String[]{"res1", "res2"});
	
	private final String key = "key";
	
	@Test
	public void shouldReturnEmptyListForMissingMapTasks() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertNotNull(pers.getMapResults());
		assertTrue(pers.getMapResults().isEmpty());
	}
	
	@Test
	public void shouldNotBeAbleToReadResultsFromOtherInstance() {
		FilePersistence pers1 = new FilePersistence(baseDir);
		pers1.initDirectories();
		FilePersistence pers2 = new FilePersistence(baseDir);
		pers2.initDirectories();
		assertTrue(pers1.storeMapResults(taskUuid, mapRes));
		assertTrue(pers2.getMapResults().isEmpty());
	}
	
	@Test
	public void shouldReturnEmptyListForMissingReduceTasks() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertNotNull(pers.getReduceResults());
		assertTrue(pers.getReduceResults().isEmpty());
	}
	
	@Test
	public void shouldReadPreviouslyStoredMapValues() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		pers.storeMapResults(taskUuid, mapRes);
		assertEquals(mapRes, pers.getMapResults());
	}
	
	@Test
	public void shouldReadPreviouslyStoredReduceValues() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		pers.storeReduceResults(taskUuid, key, redRes);
		assertEquals(redRes, pers.getReduceResults().get(key));
	}
	
	@Test
	public void shouldNotAcceptSecondMapWrite() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeMapResults(taskUuid, mapRes));
		assertFalse(pers.storeMapResults(taskUuid, mapRes));
	}
	
	@Test
	public void shouldNotAcceptSecondReduceWrite() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeReduceResults(taskUuid, key, redRes));
		assertFalse(pers.storeReduceResults(taskUuid, key, redRes));
	}
	
	@Test
	public void shouldRemoveMapResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeMapResults(taskUuid, mapRes));
		assertTrue(pers.destroyMap(taskUuid));
		assertTrue(pers.getMapResults().isEmpty());
	}
	
	@Test
	public void shouldRemoveReduceResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeReduceResults(taskUuid, key, redRes));
		assertTrue(pers.destroyReduce(taskUuid));
		assertTrue(pers.getReduceResults().isEmpty());
	}
	
	@Test
	public void shouldRemoveAllResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeMapResults(taskUuid, mapRes));
		assertTrue(pers.storeReduceResults(taskUuid, key, redRes));
		assertTrue(pers.suicide());
		assertTrue(pers.getMapResults().isEmpty());
		assertTrue(pers.getReduceResults().isEmpty());
	}
	
	@Test
	public void shouldHandleMultiMapTaskResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeMapResults("task1", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1"), new KeyValuePair("key2", "val2")})));
		assertTrue(pers.storeMapResults("task2", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key3", "val3")})));
		assertTrue(pers.storeMapResults("task3", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key4", "val4")})));
		
		List<KeyValuePair> mapResults = pers.getMapResults();
		assertEquals(4, mapResults.size());
		assertTrue(mapResults.contains(new KeyValuePair("key1", "val1")));
		assertTrue(mapResults.contains(new KeyValuePair("key2", "val2")));
		assertTrue(mapResults.contains(new KeyValuePair("key3", "val3")));
		assertTrue(mapResults.contains(new KeyValuePair("key4", "val4")));
	}
	
	@Test
	public void shouldHandleMultiReduceTaskResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeReduceResults("task1", "key1", Arrays.asList(new String[]{"res1","res2"})));
		assertTrue(pers.storeReduceResults("task2", "key2", Arrays.asList(new String[]{"res3"})));
		assertTrue(pers.storeReduceResults("task3", "key3", Arrays.asList(new String[]{"res4"})));
		
		Map<String, List<String>> redResults = pers.getReduceResults();
		assertEquals(3, redResults.size());
		assertTrue(redResults.get("key1").contains("res1"));
		assertTrue(redResults.get("key1").contains("res2"));
		assertTrue(redResults.get("key2").contains("res3"));
		assertTrue(redResults.get("key3").contains("res4"));
	}

	@Test
	public void shouldHandleMultiMapAndReduceTaskResults() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeMapResults("mapTask", mapRes));
		assertTrue(pers.storeReduceResults("resTask", key, redRes));
		assertEquals(mapRes, pers.getMapResults());
		assertEquals(redRes, pers.getReduceResults().get(key));
	}
	
	@Test
	public void shouldNotReturnDeletedReduceFiles() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeReduceResults("task1", "key1", Arrays.asList(new String[]{"res1","res2"})));
		assertTrue(pers.storeReduceResults("task2", "key2", Arrays.asList(new String[]{"res3"})));
		assertTrue(pers.storeReduceResults("task3", "key3", Arrays.asList(new String[]{"res4"})));
		
		assertTrue(pers.destroyReduce("task1"));
		assertTrue(pers.destroyReduce("task3"));
		
		Map<String, List<String>> redResults = pers.getReduceResults();
		assertEquals(1, redResults.size());
		assertTrue(redResults.get("key2").contains("res3"));
	}
	
	@Test
	public void shouldNotReturnDeletedMapFiles() {
		FilePersistence pers = new FilePersistence(baseDir);
		pers.initDirectories();
		assertTrue(pers.storeMapResults("task1", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1"), new KeyValuePair("key2", "val2")})));
		assertTrue(pers.storeMapResults("task2", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key3", "val3")})));
		assertTrue(pers.storeMapResults("task3", Arrays.asList(new KeyValuePair[]{new KeyValuePair("key4", "val4")})));
		
		assertTrue(pers.destroyMap("task1"));
		assertTrue(pers.destroyMap("task3"));
		
		List<KeyValuePair> mapResults = pers.getMapResults();
		assertEquals(1, mapResults.size());
		assertTrue(mapResults.contains(new KeyValuePair("key3", "val3")));
	}
	
	@Test
	public void shouldRemovedNestedDirs() {
		File kid = new File(this.baseDir, "kid" + System.currentTimeMillis());
		File grandkid = new File(kid, "grandkid" + System.currentTimeMillis());
		assertTrue(grandkid.mkdirs());
		assertTrue(FilePersistence.deletedir(kid));
	}
}
