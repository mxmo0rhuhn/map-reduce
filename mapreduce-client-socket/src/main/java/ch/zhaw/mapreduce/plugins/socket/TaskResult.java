package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;

public interface TaskResult {

	String getTaskUuid();

	public boolean wasSuccessful();

	public Exception getException();

	List<?> getResult();
}
