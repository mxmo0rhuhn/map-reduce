package ch.zhaw.mapreduce.android;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.workers.Worker;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Message.Builder;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;
import com.google.inject.assistedinject.Assisted;

public class AndroidWorker implements Worker {

	@Inject
	private Logger logger;

	private static final String MR_ID = "MapReduceTaskUUID";

	private static final String INPUT_ID = "Input";

	private final String apiKey;

	private final Integer timeToLive;

	private final Integer retries;

	private final String clientID;

	private final String device;

	@Inject
	public AndroidWorker(@Assisted("clientID") String clientID, @Assisted("device") String device,
			@Named("GCM_API_KEY") String apiKey, @Named("GCM_TimeToLive") Integer timeToLive,
			@Named("GCM_Retries") Integer retries) {
		this.clientID = clientID;
		this.device = device;
		this.apiKey = apiKey;
		this.timeToLive = timeToLive;
		this.retries = retries;
	}

	@Override
	public void executeTask(WorkerTask task) {
		String mrUuid = task.getMapReduceTaskUUID();
		String input = task.getInput();

		Builder b = new Message.Builder();
		b = b.timeToLive(timeToLive);
		b = b.addData(MR_ID, mrUuid);
		b = b.addData(INPUT_ID, input);
		Message msg = b.build();

		Sender sender = new Sender(apiKey);
		try {
			Result result = sender.send(msg, clientID, retries);
			// TODO interpret
		} catch (IOException e) {
			logger.log(Level.WARNING, "Failed to send GCM Message to " + this.device + " (" + this.clientID + ")", e);
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
