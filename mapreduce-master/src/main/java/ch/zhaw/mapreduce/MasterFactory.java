package ch.zhaw.mapreduce;

import com.google.inject.assistedinject.Assisted;

/**
 * Die Factory dient zum erstellen eines Masters mittels Guice und einigen Parametern.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface MasterFactory {

	/**
	 * Erstellt eine neue Instanz vom Master mit den gegebenen Parametern. Der Rest der Parameter, die der
	 * Master-Konstruktor braucht, wird per Guice aufgelöst. Alle, die dort mit Assisted annortiert sind, werden hier
	 * übergeben.
	 * 
	 * @return neue Instanz vom Master
	 */
	Master createMaster();

}
