package ch.zhaw.mapreduce.plugins.socket;

public interface TaskResult {

	String getTaskUuid();

	String getMapReduceTaskUuid();

	public boolean wasSuccessful();

	public Exception getException();
}
