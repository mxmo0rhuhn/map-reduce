package ch.zhaw.mapreduce.plugins.socket;

import java.io.Serializable;

public interface SocketTaskResult extends Serializable {
	
	boolean wasSuccessful();
	
	Exception getException();
	
	Object getResult();

}