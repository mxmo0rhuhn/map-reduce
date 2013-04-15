package ch.zhaw.mapreduce;

import java.util.List;

public interface Persistence {

	void storeMap(String mrUuid, String inputUuid, String key, String value);

	void storeReduce(String mrUuid, String inputUuid, String result);

	List<String> getReduce(String mrUuid, String inputUuid);
	
	List<KeyValuePair<String, String>> getMap(String mrUuid, String inputUuid);

	void replaceMap(String mrUuid, String inputUuid, List<KeyValuePair<String, String>> afterCombining);

	void destroy(String mrUuid, String inputUuid);

} 
