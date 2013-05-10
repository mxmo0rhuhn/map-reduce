package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des ReduceRunner mit einem WorkerPool.
 * 
 * @author Reto
 * 
 */
public class ReduceWorkerTask extends AbstractWorkerTask {

	private static final Logger LOG = Logger.getLogger(ReduceWorkerTask.class.getName());

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
	private final List<KeyValuePair> values;

	@Inject
	public ReduceWorkerTask(@Assisted("mapReduceTaskUUID") String mapReduceTaskUuid,
			@Named("taskUuid") String taskUuid, @Assisted ReduceInstruction reduceInstruction,
			@Assisted("key") String key, @Assisted List<KeyValuePair> inputs) {
		super(mapReduceTaskUuid, taskUuid);
		this.key = key;
		this.reduceInstruction = reduceInstruction;
		this.values = inputs;
	}

	/** {@inheritDoc} */
	@Override
	public void runTask(Context ctx) {
		started();

		try {
			this.reduceInstruction.reduce(ctx, key, values.iterator());
			completed();
		} catch (Exception e) {
			LOG.log(Level.WARNING, "Instruction threw Exception", e);
			failed();
			getWorker().cleanSpecificResult(getMapReduceTaskUuid(), getTaskUuid());
		}
	}

	/**
	 * Gibt den ReduceTask fuer diesen Runner zurueck.
	 * 
	 * @return Gibt den ReduceTask fuer diesen Runner zurueck. null wenn keiner gesetzt ist.
	 */
	public ReduceInstruction getReduceInstruction() {
		return this.reduceInstruction;
	}

	public List<String> getResults() {
		return getWorker().getReduceResult(getMapReduceTaskUuid(), getTaskUuid());
	}

	@Override
	public String getInput() {
		return this.key;
	}
	
	public List<KeyValuePair> getValues() {
		return this.values;
	}

	@Override
	public void abort() {
		getWorker().stopCurrentTask(getMapReduceTaskUuid(), getTaskUuid());
		aborted();
	}

}
