package ch.zhaw.mapreduce.plugins.socket;

public interface ResultCollectorObserver {
	
	public void resultAvailable(String mapReduceTaskUuid, String taskUuid, boolean success);

}
