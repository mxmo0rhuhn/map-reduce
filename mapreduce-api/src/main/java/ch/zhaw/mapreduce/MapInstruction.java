package ch.zhaw.mapreduce;

import java.io.Serializable;


/**
 * Stellt die Befehle des Maps dar. Hier muss definiert werden was genau die Map-Aufgabe des MapReduce ist.
 * 
 * @author Max
 * 
 */
public interface MapInstruction extends Serializable {

	/**
	 * Diese Methode muss überschrieben werden um die Aufgabe eines MAP Tasks zu beschreiben. Es muss eine gewisse Logik vorhanden sein mit
	 * der der gegebene Input auf KeyValue Pairs gemapped wird.
	 * 
	 * @param emitter
	 *            das runtime Environment einer jede MapInstruction. Aus ihm können verschiedene Informationen bezogen werden
	 *            und es können KeyValue-Pairs gespeichert werden.
	 * @param input
	 *            ein Input als String dieser muss in irgendeiner Art in Key Value Pairs umgewandelt werden.
	 */
	void map(MapEmitter emitter, String input);
}
