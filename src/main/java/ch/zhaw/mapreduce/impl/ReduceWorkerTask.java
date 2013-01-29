package ch.zhaw.mapreduce.impl;

import java.util.List;

import javax.inject.Inject;

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

	/**
	 * Der momentane Status
	 */
	private volatile State curState = State.INITIATED;

	@Inject
	public ReduceWorkerTask(@Assisted("uuid") String mapReduceTaskUUID,
							@Assisted("key") String key,
							@Assisted ReduceInstruction reduceInstruction,
							@Assisted List<KeyValuePair> toDo) {
		this.mapReduceTaskUUID = mapReduceTaskUUID;
		this.key = key;
		this.reduceInstruction = reduceInstruction;
		this.input = toDo;
	}

	/** {@inheritDoc} */
	@Override
	public void runTask(Context ctx) {
		this.curState = State.INPROGRESS;

		try {
			this.reduceInstruction.reduce(ctx, key, input.iterator());
			this.curState = State.COMPLETED;
		} catch (Exception e) {
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
		return this.key;
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
		return myWorker.getMapResult(mapReduceTaskUUID, key);
	}

	@Override
	public void setWorker(Worker worker) {
		this.myWorker = worker;
	}

	@Override
	public Worker getWorker() {
		return myWorker;
	}
}
