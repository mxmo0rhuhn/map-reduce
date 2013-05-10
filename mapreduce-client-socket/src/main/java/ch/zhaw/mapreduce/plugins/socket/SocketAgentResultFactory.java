package ch.zhaw.mapreduce.plugins.socket;


public interface SocketAgentResultFactory {

	SocketAgentResult createFromTaskResult(String mapReduceTaskUuid, String taskUuid, TaskResult result);

	SocketAgentResult createFromException(String mapReduceTaskUuid, String taskUuid, Exception e);

}
