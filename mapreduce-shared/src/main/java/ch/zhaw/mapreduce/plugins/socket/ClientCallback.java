package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.WorkerTask;

public interface ClientCallback {

	void acknowledge();

	Object runTask(WorkerTask task);

}
