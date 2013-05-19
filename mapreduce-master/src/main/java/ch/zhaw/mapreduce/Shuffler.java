package ch.zhaw.mapreduce;

import java.util.List;
import java.util.Map;

public interface Shuffler {

	Map<String, List<KeyValuePair>> shuffle(List<KeyValuePair> results); 

}