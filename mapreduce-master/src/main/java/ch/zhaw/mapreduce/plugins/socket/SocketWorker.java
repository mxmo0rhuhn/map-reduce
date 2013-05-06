package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;

import javax.inject.Inject;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

import com.google.inject.assistedinject.Assisted;

/**
 * @author Max
 *
 */
public class SocketWorker implements Worker {
	
	private final String ip;
	
	private final int port;
	
	private final ClientCallback callbak;
	
	@Inject
	SocketWorker(@Assisted String ip, @Assisted int port, @Assisted ClientCallback callback) {
		this.ip = ip;
		this.port = port;
		this.callbak = callback;
	}

	/** 
	 * Verbindet sich über IP & Port mit Agent, sendet Instruktionen und Input. 
	 * @see ch.zhaw.mapreduce.workers.Worker#executeTask(ch.zhaw.mapreduce.WorkerTask)
	 */
	@Override
	public void executeTask(WorkerTask task) {
		// Sich selbst als thread starten => analog ThreadWorker
				
		// Aufgabe an Agent senden 
		
		// Agent sendet ergebnis
		
		// Ergebnis in eine lokale persistenz ablegen
		
		// Worker Task auf erledigt setzen
		task.finished();
	}

	/* (non-Javadoc)
	 * @see ch.zhaw.mapreduce.workers.Worker#getReduceResult(java.lang.String, java.lang.String)
	 */
	@Override
	public List<String> getReduceResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		return null;
	}
  
	/* (non-Javadoc)
	 * @see ch.zhaw.mapreduce.workers.Worker#getMapResult(java.lang.String, java.lang.String)
	 */
	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see ch.zhaw.mapreduce.workers.Worker#cleanAllResults(java.lang.String)
	 */
	@Override
	public void cleanAllResults(String mapReduceTaskUUID) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanSpecificResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopCurrentTask(String mapReduceUUID, String taskUUID) {
		// TODO Auto-generated method stub
		
	}

	String getIp() {
		return this.ip;
	}
	
	int getPort() {
		return this.port;
	}
	
	ClientCallback getCallback() {
		return this.callbak;
	}

}
