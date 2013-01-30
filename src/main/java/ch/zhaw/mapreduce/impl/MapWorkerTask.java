package ch.zhaw.mapreduce.impl;

import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Inject;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.WorkerTask;
import ch.zhaw.mapreduce.workers.ComputationStoppedException;
import ch.zhaw.mapreduce.workers.Worker;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des MapRunners mit einem WorkerPool.
 * 
 * @author Max
 */
public class MapWorkerTask implements WorkerTask {

	private volatile Worker myWorker;
	
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

	/** Der Zustand in dem sich der Worker befindet */
	private volatile State currentState = State.INITIATED;

	@Inject
	public MapWorkerTask(@Assisted("uuid") String mapReduceTaskUID,
						 @Assisted MapInstruction mapInstruction,
						 @Assisted @Nullable CombinerInstruction combinerInstruction,
						 @Assisted("inputUUID") String inputUID,
						 @Assisted("input") String input) {
		this.mapReduceTaskUID = mapReduceTaskUID;
		this.mapInstruction = mapInstruction;
		this.combinerInstruction = combinerInstruction;
		this.inputUID = inputUID;
		this.toDo = input;
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
	public void runTask(Context ctx) {
		this.currentState = State.INPROGRESS;
		try {
			// Mappen
			this.mapInstruction.map(ctx, toDo);

			// Alle Ergebnisse verdichten. Die Ergebnisse aus der derzeitigen Worker sollen
			// einbezogen werden.
			if (this.combinerInstruction != null) {
				List<KeyValuePair> beforeCombining = ctx.getMapResult();
				List<KeyValuePair> afterCombining = this.combinerInstruction.combine(beforeCombining.iterator());
				ctx.replaceMapResult(afterCombining);
			}
			this.currentState = State.COMPLETED;
		} catch (ComputationStoppedException stopped) {
			this.currentState = State.ABORTED;
		} catch (Exception e) {
			this.currentState = State.FAILED;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getUUID() {
		return inputUID;
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
		return this.mapReduceTaskUID;
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
	public List<KeyValuePair> getResults(String mapReduceTaskUUID) {
		return myWorker.getMapResult(mapReduceTaskUID, inputUID);
	}

	@Override
	public String getInput() {
		return this.toDo;
	}
}
