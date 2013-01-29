package ch.zhaw.mapreduce.workers;


import java.util.List;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.WorkerTask;

public class AndroidWorker implements Worker {
	
	private static final String PROJECT_ID = "367594230701";
	
	private static final String API_KEY = "AIzaSyD-5CCw5L7oMij3i2OGa2Ww5Tk_YksTDyA";

	@Override
	public void executeTask(WorkerTask task) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<KeyValuePair> getReduceResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		// TODO Auto-generated method stub

	}

}
