package ch.zhaw.mapreduce.plugins.socket;


public interface TaskRunnerFactory {
	
	TaskRunner createTaskRunner(AgentTask task) throws InvalidAgentTaskException;

}
