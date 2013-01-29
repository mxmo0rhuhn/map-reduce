package ch.zhaw.mapreduce;

import java.util.List;

public interface Persistence {

	void store(String mrUuid, String inputUuid, String key, String value);

	void store(String mrUuid, String inputUuid, String result);

	List<KeyValuePair> get(String mrUuid, String inputUuid);

	void replace(String mrUuid, String inputUuid, List<KeyValuePair> afterCombining);

	void destroy(String mrUuid, String inputUuid);

} 
