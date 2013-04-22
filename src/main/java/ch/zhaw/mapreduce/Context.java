package ch.zhaw.mapreduce;

import java.util.List;

/**
 * 
 * Ein Kontext ist eine Generalisierung für die beiden Emitter Interfaces, welche einer Map- bzw. Reduce-Instruktion
 * übergeben werden für das Emitten. Das bedeutet, dass jeder Task (jeder Teil einer Berechnung) in einem Context
 * ausgeführt wird, in welchem dann die Resultate gespeichert werden.
 * 
 * Ein Context existiert pro Task.
 * 
 * @DesignReason Diese Abstraktion von einem Emitter ist notwendig, da A) der Name besser ist und B) sich ein Context
 *               auf einer anderen Maschine sich total anders verhalten muss als z.B. ein Thread auf der gleichen
 *               Maschine wieder der Master. Ein Context, der auf einer anderen Maschine als der Master läuft, muss
 *               nämlich die Resultate, sobald er danach gebeten wird, dem Master über das Netzwerk liefern.
 * 
 * @author Max Schrimpf (schrimax)
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface Context extends MapEmitter, ReduceEmitter {

	/**
	 * Liefert das Resultat für diesen MapTask. TODO A) was passiert, wenn der task noch nicht beendet ist? B) was
	 * passiert, wenn dies ein reduce task ist?
	 * 
	 * @return Resultat vom MapTask
	 * @throws ComputationStoppedException
	 *             wenn diese Berechnung gestoppt wurde
	 * @see ComputationStoppedException
	 */
	List<KeyValuePair> getMapResult() throws ComputationStoppedException;

	/**
	 * Liefert das Resultat für diesen ReduceTask. TODO A) was passiert, wenn der task noch nicht beendet ist? B) was
	 * passiert, wenn dies ein map task ist?
	 * 
	 * @return Resultat vom ReduceTask
	 * @throws ComputationStoppedException
	 *             wenn diese Berechnung gestoppt wurde
	 * @see ComputationStoppedException
	 */
	List<String> getReduceResult();

	/**
	 * Diese Methode muss aufgerufen werden, wenn eine {@link CombinerInstruction} ein Map Resultat ersetzen will. Dies
	 * tritt auf, wenn der Combiner Task diese Map Resultate nimmt und diese zu einem Map Resultat kombiniert.
	 * 
	 * @param afterCombining
	 *            das neue MapResultat
	 * @throws ComputationStoppedException
	 *             wenn diese Berechnung gestoppt wurde
	 */
	void replaceMapResult(List<KeyValuePair> afterCombining) throws ComputationStoppedException;

	void destroy();
}
