package ch.zhaw.dna.ssh.mapreduce.model.framework.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;

import ch.zhaw.dna.ssh.mapreduce.model.framework.CombinerInstruction;
import ch.zhaw.dna.ssh.mapreduce.model.framework.MapEmitter;
import ch.zhaw.dna.ssh.mapreduce.model.framework.MapWorkerTask;
import ch.zhaw.dna.ssh.mapreduce.model.framework.MapInstruction;
import ch.zhaw.dna.ssh.mapreduce.model.framework.Pool;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation des MapRunners mit einem WorkerPool.
 * 
 * @author Max
 */

public class PooledMapWorkerTask implements MapWorkerTask, MapEmitter {

	private final Pool pool;

	// Der Zustand in dem sich der Worker befindet
	private volatile State currentState = State.INITIATED;

	// Aufgabe, die der Task derzeit ausführt
	private MapInstruction mapTask;

	// Das Limit für die Anzahl an neuen Zwischenergebnissen die gewartet werden soll, bis der Combiner ausgeführt wird.
	private volatile int maxWaitResults;

	// Die Anzahl an neuen Zwischenergebnissen seit dem letzten Combiner.
	private volatile int newResults;

	// Falls vorhanden ein Combiner für die Zwischenergebnisse
	private CombinerInstruction combinerTask;

	// Die derzeit zu bearbeitenden Daten
	private volatile String toDo;

	// Ergebnisse von auf dem Worker ausgeführten MAP Tasks
	private final ConcurrentMap<String, List<String>> results = new ConcurrentHashMap<String, List<String>>();

	@Inject
	public PooledMapWorkerTask(Pool pool) {
		this.pool = pool;
	}

	/** {@inheritDoc} */
	@Override
	public void emitIntermediateMapResult(String key, String value) {
		if (!this.results.containsKey(key)) {
			this.results.putIfAbsent(key, new LinkedList<String>());
		}
		List<String> curValues = this.results.get(key);
		curValues.add(value);

		this.newResults++;

		if (this.combinerTask != null) {
			if (this.newResults >= this.maxWaitResults) {
				for (Entry<String, List<String>> entry : this.results.entrySet()) {
					ArrayList<String> resultList = new ArrayList<String>();
					resultList.add(this.combinerTask.combine(entry.getValue().iterator()));
					this.results.put(entry.getKey(), resultList);
				}
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public List<String> getIntermediate(String key) {
		return this.results.remove(key);
	}

	/** {@inheritDoc} */
	@Override
	public void runMapTask(String input) {
		this.toDo = input;
		this.currentState = State.ENQUEUED;
		this.pool.enqueueWork(this);
	}

	/** {@inheritDoc} */
	@Override
	public int getMaxWaitResults() {
		return this.maxWaitResults;
	}

	/** {@inheritDoc} */
	@Override
	public void setMaxWaitResults(int maxWaitResults) {
		this.maxWaitResults = maxWaitResults;
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
	public void doWork() {
		try {
			this.mapTask.map(this, toDo);
			this.currentState = State.COMPLETED;
		} catch (Throwable t) {
			this.currentState = State.FAILED;
		}
	}

	/** {@inheritDoc} */
	@Override
	public List<String> getKeysSnapshot() {
		return Collections.unmodifiableList(new ArrayList<String>(this.results.keySet()));
	}

	/** {@inheritDoc} */
	@Override
	@Inject
	public void setMapTask(@Assisted MapInstruction task) {
		this.mapTask = task;
	}

	/** {@inheritDoc} */
	@Override
	@Inject
	public void setCombineTask(@Assisted CombinerInstruction task) {
		this.combinerTask = task;
	}

	/** {@inheritDoc} */
	@Override
	public CombinerInstruction getCombinerTask() {
		return this.combinerTask;
	}

	/** {@inheritDoc} */
	@Override
	public MapInstruction getMapTask() {
		return this.mapTask;
	}
}