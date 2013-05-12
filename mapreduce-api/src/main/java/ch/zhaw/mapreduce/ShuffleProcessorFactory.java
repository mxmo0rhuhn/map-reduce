/**
 * 
 */
package ch.zhaw.mapreduce;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author Max
 *
 */
public interface ShuffleProcessorFactory {

	Runnable getNewRunnable(Iterator<Entry<String, List<KeyValuePair>>> results);

}
