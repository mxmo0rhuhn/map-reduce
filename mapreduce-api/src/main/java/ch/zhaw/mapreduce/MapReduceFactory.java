/**
 * 
 */
package ch.zhaw.mapreduce;

/**
 * Gibt die derzeit relevante Implementation des MapReduce Frameworks zurück.
 * @author Max
 *
 */
public final class MapReduceFactory {
	
	/** 
	 * Gibt die derzeit vorhandene Implementation des MapReduce Frameworks zurück.
	 * @return Eine Implementation des MapReduce Frameworks
	 */
	public static MapReduce getMapReduce() {
		try {
			return (MapReduce) Class.forName("ch.zhaw.mapreduce.CurrentMapReduceImplementation").newInstance();
		} catch (Exception e) {
			throw new IllegalStateException("Missing implementation of the MapReduce Framework", e);
		}
	}

}
