package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;
import java.util.List;

public interface SocketAgentResult extends Serializable {

	String getMapReduceTaskUuid();

	String getTaskUuid();

	boolean wasSuccessful();

	Exception getException();
	
	List<?> getResult();

}
