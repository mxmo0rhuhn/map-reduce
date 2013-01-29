package ch.zhaw.mapreduce.workers;


import java.io.IOException;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.WorkerTask;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Message.Builder;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;

public class AndroidWorker implements Worker {
	
	private static final String MR_ID = "MapReduceTaskUUID";
	
	private static final String INPUT_ID = "Input";
	
	@Named("GCM_API_KEY")
	@Inject
	private String apiKey;
	
	@Named("GCM_TimeToLive")
	@Inject
	private Integer gcmTimeToLive;
	
	@Named("GCM_Retries")
	@Inject
	private Integer gcmRetries;
	
	private final String clientID;
	
	public AndroidWorker(String clientID) {
		this.clientID = clientID;
	}

	@Override
	public void executeTask(WorkerTask task) {
		String mrUuid = task.getMapReduceTaskUUID();
		String input = task.getInput();
		
		Builder b = new Message.Builder();
		b = b.timeToLive(gcmTimeToLive);
		b = b.addData(MR_ID, mrUuid);
		b = b.addData(INPUT_ID, input);
		Message msg = b.build();
		
		Sender sender = new Sender(apiKey);
		try {
			Result result = sender.send(msg, clientID, gcmRetries);
		} catch (IOException e) {
			// LOG
			throw new RuntimeException("need nett!");
		}
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
