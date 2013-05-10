package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;

public interface TaskResult {

	String getTaskUuid();

	String getMapReduceTaskUuid();

	public boolean wasSuccessful();

	public Exception getException();

	List<?> getResult();
}
