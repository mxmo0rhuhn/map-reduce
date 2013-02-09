package ch.zhaw.mapreduce;

import java.util.List;


public interface Context extends MapEmitter, ReduceEmitter {

	List<KeyValuePair<String, String>> getMapResult() throws ComputationStoppedException ;

	List<String> getReduceResult();
	
	void replaceMapResult(List<KeyValuePair<String, String>> afterCombining) throws ComputationStoppedException ; 
	
	void destroy();
}
