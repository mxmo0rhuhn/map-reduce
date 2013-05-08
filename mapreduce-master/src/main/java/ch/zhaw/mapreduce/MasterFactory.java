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
	 * @param rescheduleStartPercentage
	 *            Prozentsatz der Aufgaben, die noch offen sein müssen bis rescheduled wird
	 * @param rescheduleEvery
	 *            Alle n Warte-Durchläufe wird rescheduled
	 * @param waitTime
	 *            Wartezeit in millisekunden bis in einem Durchlauf wieder die Worker angefragt werden etc
	 * @return neue Instanz vom Master
	 */
	Master createMaster(@Assisted("rescheduleStartPercentage") int rescheduleStartPercentage,
			@Assisted("rescheduleEvery") int rescheduleEvery, @Assisted("waitTime") int waitTime);

}
