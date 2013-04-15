package ch.zhaw.mapreduce;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface Shuffler {

	void put(String key, String value);

	Iterator<Map.Entry<String, List<KeyValuePair<String, String>>>> getResults(); 

}
