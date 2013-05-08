package ch.zhaw.mapreduce.plugins.socket.impl;

import ch.zhaw.mapreduce.plugins.socket.SocketTaskResult;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

public final class SocketTaskResultImpl implements SocketTaskResult {

	private static final long serialVersionUID = 7154999992933352316L;

	private final Object result;

	private final Exception exception;

	@AssistedInject
	SocketTaskResultImpl(@Assisted Exception e) {
		this.exception = e;
		this.result = null;
	}

	@AssistedInject
	SocketTaskResultImpl(@Assisted Object result) {
		this.result = result;
		this.exception = null;
	}

	@Override
	public boolean wasSuccessful() {
		return this.exception == null;
	}

	@Override
	public Exception getException() {
		return this.exception;
	}

	@Override
	public Object getResult() {
		return this.result;
	}

}
