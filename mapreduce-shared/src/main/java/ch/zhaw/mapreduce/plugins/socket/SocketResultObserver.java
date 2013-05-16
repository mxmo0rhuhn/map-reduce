package ch.zhaw.mapreduce.plugins.socket;

public interface SocketResultObserver {
	
	void resultAvailable(String taskUuid, boolean success);
	
	/**
	 * Da der Observer oft im Logging verwendet wird, sollte die toString Method überschrieben werden.
	 */
	@Override
	String toString();

}
