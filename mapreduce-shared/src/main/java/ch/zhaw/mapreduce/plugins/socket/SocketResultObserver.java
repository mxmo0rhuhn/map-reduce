package ch.zhaw.mapreduce.plugins.socket;

public interface SocketResultObserver {
	
	void resultAvailable(String taskUuid, SocketAgentResult res);
	
	/**
	 * Da der Observer oft im Logging verwendet wird, sollte die toString Method überschrieben werden.
	 */
	@Override
	String toString();

}
