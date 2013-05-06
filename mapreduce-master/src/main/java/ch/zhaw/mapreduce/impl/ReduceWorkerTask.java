package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.registry.WorkerTaskUUID;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des ReduceRunner mit einem WorkerPool.
 * 
 * @author Reto
 * 
 */
public class ReduceWorkerTask extends AbstractWorkerTask {

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
		started();

		try {
			this.reduceInstruction.reduce(ctx, key, input.iterator());
			completed();
		} catch (Exception e) {
			LOG.log(Level.WARNING, "Instruction threw Exception", e);
			failed();
			this.myWorker.cleanSpecificResult(mapReduceTaskUUID, workerTaskUuid);
		}
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
		aborted();
	}

}
