package ch.zhaw.mapreduce;

import java.util.List;


public interface Context extends MapEmitter, ReduceEmitter {

	List<KeyValuePair> getMapResult() throws ComputationStoppedException ;

	List<String> getReduceResult();
	
	void replaceMapResult(List<KeyValuePair> afterCombining) throws ComputationStoppedException ; 
	
	void destroy();
}
