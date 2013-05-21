package ch.zhaw.mapreduce.impl;

import java.util.LinkedList;
import java.util.List;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.KeyValuePair;

/**
 * Kontext für lokale Berechnungen
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class ContextImpl implements Context {
	
	private List<KeyValuePair> mapResults = new LinkedList<KeyValuePair>();
	
	private List<String> reduceResults = new LinkedList<String>();

	@Override
	public void emitIntermediateMapResult(String key, String value) {
		this.mapResults.add(new KeyValuePair(key, value));
	}

	@Override
	public void emit(String result) {
		this.reduceResults.add(result);
	}

	@Override
	public List<KeyValuePair> getMapResult() {
		return this.mapResults;
	}

	@Override
	public void replaceMapResult(List<KeyValuePair> afterCombining) {
		this.mapResults = afterCombining;
	}

	@Override
	public List<String> getReduceResult() {
		return this.reduceResults;
	}
}
