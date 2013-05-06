package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

import com.google.inject.assistedinject.Assisted;

/**
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class SocketWorker implements Worker {

	private static final Logger LOG = Logger.getLogger(SocketWorker.class.getName());

	private final String ip;

	private final int port;

	private final ClientCallback callback;

	private final ExecutorService exec;

	private final Persistence persistence;

	@Inject
	SocketWorker(@Assisted String ip, @Assisted int port, @Assisted ClientCallback callback,
			@Named("socket.workerexecutorservice") ExecutorService exec, Persistence persistence) {
		this.ip = ip;
		this.port = port;
		this.callback = callback;
		this.exec = exec;
		this.persistence = persistence;
	}

	/**
	 * Verbindet sich über IP & Port mit Agent, sendet Instruktionen und Input.
	 * 
	 * Abfolge:
	 * <ul>
	 * <li>Sich selbst als thread starten => analog ThreadWorker</li>
	 * <li>Aufgabe an Agent senden</li>
	 * <li>Agent sendet ergebnis</li>
	 * <li>Ergebnis in eine lokale persistenz ablegen</li>
	 * <li>Worker Task auf erledigt setzen</li>
	 * </ul>
	 * 
	 * @see ch.zhaw.mapreduce.workers.Worker#executeTask(ch.zhaw.mapreduce.WorkerTask)
	 */
	@Override
	public void executeTask(final WorkerTask task) {
		this.exec.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				Object result = callback.runTask(task);
				if (result == null) {
					LOG.severe("Got no result from Client");
				}
				// TODO grusig
				else if (task instanceof MapWorkerTask) {
					List<KeyValuePair> mapres = (List<KeyValuePair>) result;
					// TODO grad nochmal grusig
					for (KeyValuePair pair : mapres) {
						persistence.storeMap(task.getMapReduceTaskUUID(), task.getUUID(), (String) pair.getKey(),
								(String) pair.getValue());
					}
					task.finished();
				}

				else if (task instanceof ReduceWorkerTask) {
					List<String> redres = (List<String>) result;
					for (String res : redres) {
						persistence.storeReduce(task.getMapReduceTaskUUID(), task.getUUID(), res);
					}
					task.finished();
				}
				
				else {
					LOG.severe("task muss entweder reduce oder worker sein..");
				}
				task.failed();
				return null;
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ch.zhaw.mapreduce.workers.Worker#getReduceResult(java.lang.String, java.lang.String)
	 */
	@Override
	public List<String> getReduceResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ch.zhaw.mapreduce.workers.Worker#getMapResult(java.lang.String, java.lang.String)
	 */
	@Override
	public List<KeyValuePair> getMapResult(String mapReduceTaskUID, String inputUID) {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
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
		return this.callback;
	}

}
