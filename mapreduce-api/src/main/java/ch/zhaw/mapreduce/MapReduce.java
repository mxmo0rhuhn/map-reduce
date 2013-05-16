package ch.zhaw.mapreduce;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Dies ist die Schnittstelle zwischen einer Client - Applikation und dem MapReduce Framework.
 * 
 * @author Max,
 */
public interface MapReduce {

	/**
	 * Startet die eingebundene Implementation des MapReduce - sei es ein Server oder ein client.
	 */
	void start();

	/**
	 * Stoppt die eingebundene Implementation des MapReduce Frameworks.
	 */
	void stop();

	/**
	 * Stellt das Framework auf diese dedizierte Aufgabe ein
	 * 
	 * @param mapInstruction
	 *            eine Map Anweisung, die ausgeführt werden soll
	 * @param reduceInstruction
	 *            eine Reduce Anweisung, die ausgeführt werden soll
	 * @param combinerInstruction
	 *            eine optionale combiner Instruction, die ausgeführt werden soll
	 * @param shuffleProcessorFactory
	 *            Eine Factory die runnables zurückgibt, von denen jeweils einer ausgeführt wird mit den ergebnissen der shuffle Phase ausgeführt wird, sobald die shuffle Phase beendet ist.
	 */
	MapReduce newMRTask(MapInstruction mapInstruction, ReduceInstruction reduceInstruction,
			CombinerInstruction combinerInstruction, ShuffleProcessorFactory shuffleProcessorFactory);

	/**
	 * Wendet auf alle Elemente vom übergebenen Iterator (via next) den Map- und Reduce-Task an. Die
	 * Methode blockiert, bis alle Aufgaben erledigt sind. Es wird über den Iterator iteriert und
	 * für jeden Aufruf von {@link Iterator#next()} ein Map-Task abgesetzt (asynchron). Aus diesem
	 * Grund könnten im Iterator die Werte auf lazy generiert werden.
	 * 
	 * @param inputs
	 *            der gesamte input als Iterator
	 * @return das Resultat von dem ganzen MapReduceTask
	 */
	Map<String, List<String>> runMapReduceTask(Iterator<String> input);
}
