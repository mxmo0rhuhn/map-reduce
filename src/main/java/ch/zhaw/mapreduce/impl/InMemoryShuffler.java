package ch.zhaw.mapreduce.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Shuffler;

public class InMemoryShuffler implements Shuffler {

	private final Map<String, List<KeyValuePair>> mapResults = new HashMap<String, List<KeyValuePair>>();

	@Override
	public void put(String key, String value) {
		if (mapResults.containsKey(key)) {
			mapResults.get(key).add(new KeyValuePair(key, value));
		} else {
			List<KeyValuePair> newKeyValueList = new LinkedList<KeyValuePair>();
			newKeyValueList.add(new KeyValuePair(key, value));
			mapResults.put(key, newKeyValueList);
		}
	}

	@Override
	public Iterator<Entry<String, List<KeyValuePair>>> getResults() {
		return mapResults.entrySet().iterator();
	}
}