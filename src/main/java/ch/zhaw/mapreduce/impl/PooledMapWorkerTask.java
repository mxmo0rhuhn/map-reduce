package ch.zhaw.mapreduce.impl;

import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Inject;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapEmitter;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.MapWorkerTask;
import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.Worker;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des MapRunners mit einem WorkerPool.
 * 
 * @author Max
 */
public class PooledMapWorkerTask implements MapWorkerTask, MapEmitter {

	/** Der verwendete Pool */
	private final Pool pool;

	/** Aufgabe, die der Task derzeit ausführt */
	private final MapInstruction mapInstruction;

	/** Falls vorhanden ein Combiner für die Zwischenergebnisse */
	private final CombinerInstruction combinerInstruction;

	/** Eine eindeutige ID die jeder Map Reduce Task besitzt */
	private final String mapReduceTaskUID;

	/** Die derzeit zu bearbeitenden Daten */
	private final String toDo;

	/** Die eindeutihe ID die jeder input besitzt */
	private final String inputUID;

	/** Der den Task ausführende Worker */
	private volatile Worker processingWorker;

	/** Der Zustand in dem sich der Worker befindet */
	private volatile State currentState = State.INITIATED;

	@Inject
	public PooledMapWorkerTask(Pool pool,
						       @Assisted("uuid") String mapReduceTaskUID,
							   @Assisted MapInstruction mapInstruction,
							   @Assisted @Nullable CombinerInstruction combinerInstruction,
							   @Assisted("inputUUID") String inputUID,
							   @Assisted("input") String input) {
		this.pool = pool;
		this.mapReduceTaskUID = mapReduceTaskUID;
		this.mapInstruction = mapInstruction;
		this.combinerInstruction = combinerInstruction;
		this.inputUID = inputUID;
		this.toDo = input;
	}

	/** {@inheritDoc} */
	@Override
	public void emitIntermediateMapResult(String key, String value) {
		processingWorker.storeMapResult(mapReduceTaskUID, new KeyValuePair(key, value));
	}

	/** {@inheritDoc} */
	@Override
	public void runMapTask() {
		this.currentState = State.ENQUEUED;
		this.pool.enqueueWork(this);
	}

	/**
	 * {@inheritDoc} Diese Angabe ist optimistisch. Sie kann veraltet sein.
	 */
	@Override
	public State getCurrentState() {
		return this.currentState;
	}

	/** {@inheritDoc} */
	@Override
	public void doWork(Worker processingWorker) {
		this.currentState = State.INPROGRESS;
		this.processingWorker = processingWorker;
		try {
			// Mappen
			this.mapInstruction.map(this, toDo);

			// Alle Ergebnisse verdichten. Die Ergebnisse aus der derzeitigen Worker sollen
			// einbezogen werden.
			if (this.combinerInstruction != null) {
				List<KeyValuePair> vals = processingWorker.getMapResults(mapReduceTaskUID);
				if (vals != null) {
					this.combinerInstruction.combine(vals.iterator());
				}
			}

			this.currentState = State.COMPLETED;
		} catch (Exception e) {
			this.currentState = State.FAILED;
			this.processingWorker = null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Worker getWorker() {
		return processingWorker;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getUUID() {
		return inputUID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MapInstruction getMapInstruction() {
		return this.mapInstruction;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public CombinerInstruction getCombinerInstruction() {
		return this.combinerInstruction;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getMapReduceTaskUUID() {
		return this.mapReduceTaskUID;
	}
}
