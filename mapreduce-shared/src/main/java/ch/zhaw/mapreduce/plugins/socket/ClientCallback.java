package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;

import ch.zhaw.mapreduce.WorkerTask;

public interface ClientCallback extends Serializable {

	void helloslave();

	Object runTask(WorkerTask task);
	
	String getIp();

}
