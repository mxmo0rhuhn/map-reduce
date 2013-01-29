package ch.zhaw.mapreduce;

import java.util.List;

import ch.zhaw.mapreduce.workers.ComputationStoppedException;

public interface Context extends MapEmitter, ReduceEmitter {

	List<KeyValuePair> getMapResult() throws ComputationStoppedException ;

	void replaceMapResult(List<KeyValuePair> afterCombining) throws ComputationStoppedException ; 
	
	void destroy();
}
