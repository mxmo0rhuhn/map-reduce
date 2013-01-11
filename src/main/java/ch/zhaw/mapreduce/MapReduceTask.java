package ch.zhaw.mapreduce;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import ch.zhaw.mapreduce.registry.Registry;

/**
 * Dies ist ein neuer MapReduce Task. Er ist für seine Worker der Master.
 * 
 * @author Max, Reto, Desiree
 */
public final class MapReduceTask {

	private final MapInstruction mapInstruction;

	private final ReduceInstruction reduceInstruction;

	private final CombinerInstruction combinerInstruction;

	private final Master master;

	/**
	 * Erstellt einen neuen MapReduceTask mit den übergebenen map und reduce tasks.
	 * 
	 * @param mapInstruction
	 *            die MapInstruction
	 * @param reduceInstruction
	 *            die ReduceInstruction
	 */
	public MapReduceTask(MapInstruction mapInstruction, ReduceInstruction reduceInstruction, CombinerInstruction combinerInstruction) {
		this.mapInstruction = mapInstruction;
		this.reduceInstruction = reduceInstruction;
		this.combinerInstruction = combinerInstruction;

		this.master = Registry.getComponent(Master.class);
	}

	public MapReduceTask(MapInstruction mapInstruction, ReduceInstruction reduceInstruction) {
		this(mapInstruction, reduceInstruction, null);
	}

	/**
	 * Wendet auf alle Elemente vom übergebenen Iterator (via next) den Map- und Reduce-Task an. Die Methode blockiert,
	 * bis alle Aufgaben erledigt sind. Es wird über den Iterator iteriert und für jeden Aufruf von
	 * {@link Iterator#next()} ein Map-Task abgesetzt (asynchron). Aus diesem Grund könnten im Iterator die Werte auf
	 * lazy generiert werden.
	 * 
	 * @param inputs
	 *            der gesamte input als Iterator
	 * @return das Resultat von dem ganzen MapReduceTask
	 */
	public Map<String, String> compute(Iterator<String> input) throws InterruptedException {
		return this.master.runComputation(this.mapInstruction, this.combinerInstruction, this.reduceInstruction, input);
	}
}
