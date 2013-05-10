package ch.zhaw.mapreduce.plugins.socket;

public interface SocketResultObserver {
	
	public void resultAvailable(String mapReduceTaskUuid, String taskUuid, boolean success);

}
