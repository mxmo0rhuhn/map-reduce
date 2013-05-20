package ch.zhaw.mapreduce.impl;

import java.util.logging.Logger;

import ch.zhaw.mapreduce.Persistence;
import ch.zhaw.mapreduce.WorkerTask;

/**
 * Abstrakter WorkerTask, der das State-Handling implementiert, den Worker verwaltet und die Uuid speichert.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
abstract class AbstractWorkerTask implements WorkerTask {

	private static final Logger LOG = Logger.getLogger(AbstractWorkerTask.class.getName());

	private final String taskUuid;
	
	protected final Persistence persistence;

	/** Der Zustand in dem sich der Worker befindet */
	private volatile State currentState = State.INITIATED;

	AbstractWorkerTask(String taskUuid, Persistence pers) {
		this.taskUuid = taskUuid;
		this.persistence = pers;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final String getTaskUuid() {
		return this.taskUuid;
	}
	
	@Override
	public final Persistence getPersistence() {
		return this.persistence;
	}


	/**
	 * {@inheritDoc} Diese Angabe ist optimistisch. Sie kann veraltet sein.
	 */
	@Override
	public final State getCurrentState() {
		return this.currentState;
	}

	@Override
	public final void enqueued() {
		setState(State.ENQUEUED);
	}

	@Override
	public final void started() {
		setState(State.INPROGRESS);
	}

	protected final void failed() {
		setState(State.FAILED);
	}

	protected final void completed() {
		setState(State.COMPLETED);
	}

	protected final void aborted() {
		setState(State.ABORTED);
	}
	
	/**
	 * Setzt den neuen State als State. Der neue State darf nicht der gleich sein.
	 */
	public final void setState(State newState) {
		if (this.currentState == newState) {
			throw new IllegalStateException("Task was already in State: " + newState);
		}
		LOG.finest("From " + this.currentState + " to " + newState);
		this.currentState = newState;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[TaskUuid=" + this.taskUuid + ",State=" + this.currentState + "]";
	}

}
