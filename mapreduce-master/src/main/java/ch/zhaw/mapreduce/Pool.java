package ch.zhaw.mapreduce;

/**
 * Die zentrale Einheit, die im wesentlichen Tasks auf Worker verteilt. Hier müssen sich alle Worker melden und jeder
 * Task wird hier eingereiht, dann wird er vom Pool an einen freien Worker übergeben.
 * 
 * @author Max Schrimpf (schrimax)
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface Pool {

	/**
	 * Worker dem Pool hinzufügen.
	 * 
	 * @return true, wenn der Worker akzeptiert wurde. sonst false
	 */
	boolean donateWorker(Worker newWorker);

	/**
	 * Neuen Task zur Ausführung aufgeben.
	 * 
	 * @return true, wenn der task akzeptiert wurde. sonst false.
	 */
	boolean enqueueTask(WorkerTask task);

	/**
	 * Nachdem der Worker einen Task ausgeführt hat, muss er sich selbst beim Pool zurückmelden.
	 * 
	 * @return true, wenn der worker akzeptiert wurde. sonst false (wenn sich der worker z.B. vorhin nicht registriert
	 *         hat).
	 */
	boolean workerIsFinished(Worker finishedWorker);

	/**
	 * Einen Worker aus dem Pool entfernen.
	 */
	void iDied(Worker deadWorker);

	/**
	 * Anzahl eingereihter Tasks.
	 */
	int enqueuedTasks();

	/**
	 * Anzahl Worker die nichts zu tun haben bzw. auf Arbeit warten.
	 */
	int getFreeWorkers();

	/**
	 * Anzahl Worker die dem Pool bekannt sind. Also die Summe aus arbeitenden und auf-arbeit-wartenden.
	 */
	int getCurrentPoolSize();
	
	/**
	 * Anzahl Tasks, die dieser Pool schon ausgefuehrt hat.
	 * @return
	 */
	long totalRunTasks();
}
