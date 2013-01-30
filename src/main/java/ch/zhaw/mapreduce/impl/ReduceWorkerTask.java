package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.ComputationStoppedException;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.workers.Worker;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des ReduceRunner mit einem WorkerPool.
 * 
 * @author Reto
 * 
 */
public class ReduceWorkerTask implements WorkerTask {

	@Inject
	private Logger logger;

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
	
	private final String reduceTaskUuid;

	/**
	 * Der momentane Status
	 */
	private volatile State curState = State.INITIATED;

	@Inject
	public ReduceWorkerTask(@Assisted("uuid") String mapReduceTaskUUID, @Assisted("reduceTaskUuid") String reduceTaskUuid, @Assisted ReduceInstruction reduceInstruction,
			@Assisted("key") String key, @Assisted List<KeyValuePair> inputs) {
		this.mapReduceTaskUUID = mapReduceTaskUUID;
		this.key = key;
		this.reduceInstruction = reduceInstruction;
		this.input = inputs;
		this.reduceTaskUuid = reduceTaskUuid;
	}

	/** {@inheritDoc} */
	@Override
	public void runTask(Context ctx) {
		this.curState = State.INPROGRESS;
		logger.finest("State: INPROGRESS");

		try {
			this.reduceInstruction.reduce(ctx, key, input.iterator());
			this.curState = State.COMPLETED;
			logger.finest("State: COMPLETED");
		} catch (ComputationStoppedException cse) {
			logger.finest("State: ABORTED");
			this.curState = State.ABORTED;
		} catch (Exception e) {
			logger.log(Level.WARNING, "State: FAILED", e);
			this.curState = State.FAILED;
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
		return this.reduceTaskUuid;
	}

	/**
	 * Gibt den ReduceTask fuer diesen Runner zurueck.
	 * 
	 * @return Gibt den ReduceTask fuer diesen Runner zurueck. null wenn keiner gesetzt ist.
	 */
	public ReduceInstruction getReduceTask() {
		return this.reduceInstruction;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getMapReduceTaskUUID() {
		return this.mapReduceTaskUUID;
	}

	public List<KeyValuePair> getResults(String mapReduceTaskUUID) {
		return myWorker.getMapResult(mapReduceTaskUUID, reduceTaskUuid);
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
}
