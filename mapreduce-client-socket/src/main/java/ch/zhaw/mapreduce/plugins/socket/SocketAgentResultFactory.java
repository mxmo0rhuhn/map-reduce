package ch.zhaw.mapreduce.plugins.socket;


public interface SocketAgentResultFactory {

	SocketAgentResult createFromTaskResult(TaskResult result);

	SocketAgentResult createFromException(Exception e);

}
