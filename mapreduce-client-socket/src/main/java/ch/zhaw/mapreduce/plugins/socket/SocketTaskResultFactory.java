package ch.zhaw.mapreduce.plugins.socket;



public interface SocketTaskResultFactory {

	SocketTaskResult createSuccessResult(Object o);

	SocketTaskResult createFailureResult(Exception e);

}
