/**
 * 
 */
package ch.zhaw.mapreduce;

import java.util.Iterator;
import java.util.List;

/**
 * Die nötigen Befehle um Zwischenergebnisse in einem Map Task zu aggregieren bevor diese vom Reduce abgefragt werden.
 * Dies könnte in konkreten Implementationen Netzwerktraffic sparen.
 * 
 * @author Max
 *
 */
public interface CombinerInstruction {

	/**
	 * Führt eine Liste mit Values des selben Keys zusammen
	 * 
	 * @param toCombine Die Liste mit values
	 * @return der aggregierte Wert.
	 */
	List<KeyValuePair> combine(Iterator<KeyValuePair> toCombine);
}
