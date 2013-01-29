package ch.zhaw.mapreduce;

import java.util.List;

public class FilePersistence implements Persistence {

	@Override
	public void store(String mrUuid, String inputUuid, String key, String value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void store(String mrUuid, String inputUuid, String result) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<KeyValuePair> get(String mrUuid, String inputUuid) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void replace(String mrUuid, String inputUuid, List<KeyValuePair> afterCombining) {
		// TODO Auto-generated method stub

	}

	@Override
	public void destroy(String mrUuid, String inputUuid) {
		// TODO Auto-generated method stub

	}

}
