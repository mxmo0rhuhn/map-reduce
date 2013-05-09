package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;

import ch.zhaw.mapreduce.plugins.socket.SocketAgentResult;

public class SocketAgentResultImpl implements SocketAgentResult {

	@Override
	public String getMapReduceTaskUuid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTaskUuid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean wasSuccessful() {
		return this.result != null;
	}

	@Override
	public Exception getException() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<?> getResult() {
		// TODO Auto-generated method stub
		return null;
	}


}
