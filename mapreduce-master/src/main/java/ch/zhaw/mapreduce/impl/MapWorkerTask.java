package ch.zhaw.mapreduce.impl;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.Worker;
import ch.zhaw.mapreduce.registry.WorkerTaskUUID;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des MapRunners mit einem WorkerPool.
 * 
 * @author Max
 */
public class MapWorkerTask extends AbstractWorkerTask {

	private static final long serialVersionUID = -6784218417643187180L;

	private static final Logger LOG = Logger.getLogger(MapWorkerTask.class.getName());

	/** transient weil wir ihn nicht auf den client propagieren wollen */
	private transient volatile Worker worker;

	/** Aufgabe, die der Task derzeit ausführt */
	private final MapInstruction mapInstruction;

	/** Falls vorhanden ein Combiner für die Zwischenergebnisse */
	private final CombinerInstruction combinerInstruction;

	/** Eine eindeutige ID die jeder Map Reduce Task besitzt */
	private final String mapReduceTaskUuid;

	/** Die derzeit zu bearbeitenden Daten */
	private final String input;

	/** Die eindeutihe ID die jeder input besitzt */
	private final String taskUuid;

	@Inject
	public MapWorkerTask(@Assisted("mapReduceTaskUUID") String mapReduceTaskUUID,
			@WorkerTaskUUID String taskUuid, 
			@Assisted MapInstruction mapInstruction,
			@Assisted @Nullable CombinerInstruction combinerInstruction,
			@Assisted("input") String input) {
		this.mapReduceTaskUuid = mapReduceTaskUUID;
		this.mapInstruction = mapInstruction;
		this.combinerInstruction = combinerInstruction;
		this.taskUuid = taskUuid;
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
			this.worker.cleanSpecificResult(mapReduceTaskUuid, taskUuid);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getUUID() {
		return taskUuid;
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getMapReduceTaskUUID() {
		return this.mapReduceTaskUuid;
	}

	@Override
	public void setWorker(Worker worker) {
		this.worker = worker;
	}

	@Override
	public Worker getWorker() {
		return worker;
	}

	public List<KeyValuePair> getResults() {
		return worker.getMapResult(mapReduceTaskUuid, taskUuid);
	}

	@Override
	public String getInput() {
		return this.input;
	}

	@Override
	public void abort() {
		aborted();
		this.worker.stopCurrentTask(mapReduceTaskUuid, taskUuid);
	}

}
