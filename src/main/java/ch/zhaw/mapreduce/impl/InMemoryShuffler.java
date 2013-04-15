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

	private final Map<String, List<KeyValuePair<String, String>>> mapResults = new HashMap<String, List<KeyValuePair<String, String>>>();

	@Override
	public void put(String key, String value) {
		if (mapResults.containsKey(key)) {
			mapResults.get(key).add(new KeyValuePair<String, String>(key, value));
		} else {
			List<KeyValuePair<String, String>> newKeyValueList = new LinkedList<KeyValuePair<String, String>>();
			newKeyValueList.add(new KeyValuePair<String, String>(key, value));
			mapResults.put(key, newKeyValueList);
		}
	}

	@Override
	public Iterator<Entry<String, List<KeyValuePair<String, String>>>> getResults() {
		return mapResults.entrySet().iterator();
	}
}