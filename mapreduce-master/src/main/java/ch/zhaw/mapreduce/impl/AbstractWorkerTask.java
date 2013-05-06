package ch.zhaw.mapreduce.impl;

import java.util.logging.Logger;

import ch.zhaw.mapreduce.WorkerTask;

/**
 * Abstrakte WorkerTask, der das State-Handling implementiert.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public abstract class AbstractWorkerTask implements WorkerTask {

	private static final Logger LOG = Logger.getLogger(AbstractWorkerTask.class.getName());

	/** Der Zustand in dem sich der Worker befindet */
	private volatile State currentState = State.INITIATED;

	/**
	 * {@inheritDoc} Diese Angabe ist optimistisch. Sie kann veraltet sein.
	 */
	@Override
	public final State getCurrentState() {
		return this.currentState;
	}

	@Override
	public final void finished() {
		setState(State.COMPLETED);
	}

	@Override
	public final void enqueued() {
		setState(State.ENQUEUED);
	}

	@Override
	public final void started() {
		setState(State.INPROGRESS);
	}

	@Override
	public final void failed() {
		setState(State.FAILED);
	}

	@Override
	public final void completed() {
		setState(State.COMPLETED);
	}

	@Override
	public final void aborted() {
		setState(State.ABORTED);
	}

	/**
	 * Setzt den neuen State als State. Der neue State darf nicht der gleich sein.
	 */
	private void setState(State newState) {
		if (this.currentState == newState) {
			throw new IllegalStateException("Task was already in State: " + newState);
		}
		LOG.finest("From " + this.currentState + " to " + newState);
		this.currentState = newState;
	}

}
