package ch.zhaw.mapreduce.impl;

import java.util.List;

import javax.inject.Inject;

import ch.zhaw.mapreduce.ComputationStoppedException;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.Persistence;

import com.google.inject.assistedinject.Assisted;

/**
 * Kontext für lokale Berechnungen
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class LocalContext implements Context {

	/**
	 * Die Resultate von den ThreadWorker werden nicht In-Memory gehalten sondern über die Persistence gespeichert.
	 * Typischerweise dürfte das eine Datei sein.
	 */
	private final Persistence persistence;
	
	private final String mrUuid;
	
	private final String taskUuid;
	
	private volatile boolean stopped = false;

	@Inject
	LocalContext(Persistence persistence, @Assisted("mapReduceTaskUUID") String mrUuid, @Assisted("taskUUID") String taskUuid) {
		this.mrUuid = mrUuid;
		this.taskUuid = taskUuid;
		this.persistence = persistence;
	}

	@Override
	public void emitIntermediateMapResult(String key, String value) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.storeMap(mrUuid, taskUuid, key, value);
	}

	@Override
	public void emit(String result) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.storeReduce(mrUuid, taskUuid, result);
	}

	@Override
	public List<KeyValuePair> getMapResult() {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		return persistence.getMap(mrUuid, taskUuid);
	}

	@Override
	public void replaceMapResult(List<KeyValuePair> afterCombining) {
		if (stopped) {
			throw new ComputationStoppedException();
		}
		persistence.replaceMap(mrUuid, taskUuid, afterCombining);
	}

	@Override
	public void destroy() {
		stopped = true;
		persistence.destroy(mrUuid, taskUuid);
	}

	@Override
	public List<String> getReduceResult() {
		return persistence.getReduce(mrUuid, taskUuid);
	}

	@Override
	public String getMapReduceTaskUUID() {
		return mrUuid;
	}

	@Override
	public String getTaskUUID() {
		return taskUuid;
	}
}
