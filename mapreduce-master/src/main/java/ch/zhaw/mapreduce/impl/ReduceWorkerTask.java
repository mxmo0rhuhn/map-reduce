package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.registry.WorkerTaskUUID;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des ReduceRunner mit einem WorkerPool.
 * 
 * @author Reto
 * 
 */
public class ReduceWorkerTask implements WorkerTask {

	private static final Logger LOG = Logger.getLogger(ReduceWorkerTask.class.getName());

	private volatile Worker myWorker;
	/**
	 * Die globale MapReduce-Berechnungs-ID zu der dieser Task gehoert
	 */
	private final String mapReduceTaskUUID;

	/**
	 * Fuer diesen Key wollen wir reduzieren
	 */
	private final String key;

	/**
	 * Diese ReduceInstruction wird angewendet
	 */
	private final ReduceInstruction reduceInstruction;

	/**
	 * Der zu reduzierende Input
	 */
	private final List<KeyValuePair> input;

	private final String workerTaskUuid;

	/**
	 * Der momentane Status
	 */
	private volatile State curState = State.INITIATED;

	@Inject
	public ReduceWorkerTask(@Assisted("mapReduceTaskUUID") String mapReduceTaskUUID,
			@Assisted ReduceInstruction reduceInstruction, @Assisted("key") String key,
			@Assisted List<KeyValuePair> inputs, @WorkerTaskUUID String workerTaskUuid) {
		this.mapReduceTaskUUID = mapReduceTaskUUID;
		this.key = key;
		this.reduceInstruction = reduceInstruction;
		this.input = inputs;
		this.workerTaskUuid = workerTaskUuid;
	}

	/** {@inheritDoc} */
	@Override
	public void runTask(Context ctx) {
		this.curState = State.INPROGRESS;
		LOG.finest("State: INPROGRESS");

		try {
			this.reduceInstruction.reduce(ctx, key, input.iterator());
			this.curState = State.COMPLETED;
			LOG.finest("State: COMPLETED");
		} catch (Exception e) {
			LOG.log(Level.WARNING, "State: FAILED", e);
			this.curState = State.FAILED;
			this.myWorker.cleanSpecificResult(mapReduceTaskUUID, workerTaskUuid);
		}
	}

	/** {@inheritDoc} */
	@Override
	public State getCurrentState() {
		return this.curState;
	}

	/** {@inheritDoc} */
	@Override
	public String getUUID() {
		return this.workerTaskUuid;
	}

	/**
	 * Gibt den ReduceTask fuer diesen Runner zurueck.
	 * 
	 * @return Gibt den ReduceTask fuer diesen Runner zurueck. null wenn keiner gesetzt ist.
	 */
	public ReduceInstruction getReduceInstruction() {
		return this.reduceInstruction;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getMapReduceTaskUUID() {
		return this.mapReduceTaskUUID;
	}

	public List<String> getResults(String mapReduceTaskUUID) {
		return myWorker.getReduceResult(mapReduceTaskUUID, workerTaskUuid);
	}

	@Override
	public void setWorker(Worker worker) {
		this.myWorker = worker;
	}

	@Override
	public Worker getWorker() {
		return myWorker;
	}

	@Override
	public String getInput() {
		return this.key;
	}

	@Override
	public void abort() {
		this.myWorker.stopCurrentTask(mapReduceTaskUUID, workerTaskUuid);
		this.curState = State.ABORTED;
	}

	@Override
	public void finished() {
		this.curState = State.COMPLETED;
	}

	@Override
	public void enqueued() {
		this.curState = State.ENQUEUED;
	}
}
