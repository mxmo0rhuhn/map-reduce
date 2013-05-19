package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Persistence;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des MapRunners mit einem WorkerPool.
 * 
 * @author Max
 */
public class MapWorkerTask extends AbstractWorkerTask {

	private static final Logger LOG = Logger.getLogger(MapWorkerTask.class.getName());
	
	private final Persistence persistence;

	/** Aufgabe, die der Task derzeit ausführt */
	private final MapInstruction mapInstruction;

	/** Falls vorhanden ein Combiner für die Zwischenergebnisse */
	private final CombinerInstruction combinerInstruction;

	/** Die derzeit zu bearbeitenden Daten */
	private final String input;

	@Inject
	public MapWorkerTask(@Named("taskUuid") String taskUuid, 
			@Assisted Persistence persistence,
			@Assisted MapInstruction mapInstruction,
			@Assisted @Nullable CombinerInstruction combinerInstruction,
			@Assisted("input") String input) {
		super(taskUuid);
		this.persistence = persistence;
		this.mapInstruction = mapInstruction;
		this.combinerInstruction = combinerInstruction;
		this.input = input;
	}

	/** {@inheritDoc} */
	@Override
	public void runTask(Context ctx) {
		started();
		try {
			// Mappen
			this.mapInstruction.map(ctx, input);

			// Alle Ergebnisse verdichten. Die Ergebnisse aus der derzeitigen Worker sollen
			// einbezogen werden.
			if (this.combinerInstruction != null) {
				List<KeyValuePair> beforeCombining = ctx.getMapResult();
				List<KeyValuePair> afterCombining = this.combinerInstruction
						.combine(beforeCombining.iterator());
				ctx.replaceMapResult(afterCombining);
			}
			completed();
		} catch (Exception e) {
			LOG.log(Level.WARNING, "Instruction threw Exception", e);
			failed();
			this.persistence.destroy(getTaskUuid());
		}
	}

	/**
	 * Gibt die verwendete MapInstruciton zurueck.
	 * 
	 * @return verwendete MapInstruction
	 */
	public MapInstruction getMapInstruction() {
		return this.mapInstruction;
	}

	/**
	 * Liefert die verwendete CombinerInstruction
	 * 
	 * @return die verwendete CombinerInstruction. null wenn keine verwendet wurde.
	 */
	public CombinerInstruction getCombinerInstruction() {
		return this.combinerInstruction;
	}

	public List<KeyValuePair> getResults() {
		return this.persistence.getMapResults(getTaskUuid());
	}

	@Override
	public String getInput() {
		return this.input;
	}

	@Override
	public void abort() {
		aborted();
		this.persistence.destroy(getTaskUuid());
	}

	@Override
	public void successful(List<?> result) {
		if (result != null && !result.isEmpty()) {
			List<KeyValuePair> typedResult;
			try {
				typedResult = (List<KeyValuePair>) result;
				this.persistence.storeMapResults(getTaskUuid(), typedResult);
				completed();
			} catch (ClassCastException e) {
				LOG.log(Level.SEVERE, "Wrong type for MapTask", e);
				failed();
				return;
			}
		}
	}

}
