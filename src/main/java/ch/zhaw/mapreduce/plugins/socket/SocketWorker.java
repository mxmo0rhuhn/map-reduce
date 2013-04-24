package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

/**
 * @author Max
 *
 */
public class SocketWorker implements Worker {

	/** 
	 * Verbindet sich über IP & Port mit Agent, sendet Instruktionen und Input. 
	 * @see ch.zhaw.mapreduce.workers.Worker#executeTask(ch.zhaw.mapreduce.WorkerTask)
	 */
	@Override
	public void executeTask(WorkerTask task) {
		// Aufgabe an Agent senden 
		
		// Agent sendet ergebnis
		
		// Ergebnis in eine lokale persistenz ablegen
		
		// Worker Task auf erledigt setzen
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

}
