package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.ComputationStoppedException;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.WorkerTask;

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
	public ReduceWorkerTask(@Assisted("mapReduceTaskUUID") String mapReduceTaskUUID,  @Assisted ReduceInstruction reduceInstruction,
			@Assisted("key") String key, @Assisted List<KeyValuePair> inputs) {
		this.mapReduceTaskUUID = mapReduceTaskUUID;
		this.key = key;
		this.reduceInstruction = reduceInstruction;
		this.input = inputs;
		this.workerTaskUuid = UUID.randomUUID().toString();
	}

	/** {@inheritDoc} */
	@Override
	public void runTask(Context ctx) {
		this.curState = State.INPROGRESS;
		LOG.finest("State: INPROGRESS");

		try {
			this.reduceInstruction.reduce(ctx, key, input.iterator());
			
			if( curState == State.INPROGRESS) {
				this.curState = State.COMPLETED;
				LOG.finest("State: COMPLETED");
			} else {
				throw new ComputationStoppedException();
			}
			
		} catch (ComputationStoppedException cse) {
			LOG.finest("State: ABORTED");
			this.curState = State.ABORTED;
			this.myWorker.cleanSpecificResult(mapReduceTaskUUID, workerTaskUuid);
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
	public void setState(State newState) {
		this.curState = newState;
	}
}
