package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;

public interface AgentTask extends Serializable {

	public abstract String getTaskUuid();

	public abstract String getMapReduceTaskUuid();

}
