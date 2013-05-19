package ch.zhaw.mapreduce.impl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Shuffler;

public class InMemoryShuffler implements Shuffler {

	@Override
	public Map<String, List<KeyValuePair>> shuffle(List<KeyValuePair> results) {
		Map<String, List<KeyValuePair>> mapResults = new HashMap<String, List<KeyValuePair>>();
		for (KeyValuePair pair : results) {
			String key = (String) pair.getKey();
			String value = (String) pair.getValue();
			if (mapResults.containsKey(key)) {
				mapResults.get(key).add(new KeyValuePair(key, value));
			} else {
				List<KeyValuePair> newKeyValueList = new LinkedList<KeyValuePair>();
				newKeyValueList.add(new KeyValuePair(key, value));
				mapResults.put(key, newKeyValueList);
			}
		}
		return mapResults;
	}
}