package ch.zhaw.mapreduce.impl;

import java.util.logging.Logger;

import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

/**
 * Abstrakter WorkerTask, der das State-Handling implementiert, den Worker verwaltet und die Uuid speichert.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
abstract class AbstractWorkerTask implements WorkerTask {

	private static final long serialVersionUID = 7421579837577817746L;

	private static final Logger LOG = Logger.getLogger(AbstractWorkerTask.class.getName());
	
	private final String mapReduceTaskUuid;
	
	private final String taskUuid;

	/** Der Zustand in dem sich der Worker befindet */
	private volatile State currentState = State.INITIATED;
	
	private transient volatile Worker worker;

	AbstractWorkerTask(String mapReduceTaskUuid, String taskUuid) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Worker getWorker() {
		return this.worker;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void setWorker(Worker worker) {
		this.worker = worker;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final String getMapReduceTaskUuid() {
		return this.mapReduceTaskUuid;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public final String getTaskUuid() {
		return this.taskUuid;
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
