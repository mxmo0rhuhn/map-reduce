package ch.zhaw.mapreduce.plugins.socket;


public interface SocketAgentResultFactory {

	SocketAgentResult createFromTaskResult(String taskUuid, TaskResult result);

	SocketAgentResult createFromException(String taskUuid, Exception e);

}
