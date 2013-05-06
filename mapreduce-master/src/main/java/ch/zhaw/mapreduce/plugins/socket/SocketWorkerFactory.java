package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.Worker;

/**
 * Factory um neue SocketWorker zu erstellen. Die Factory basiert auf AssistedInject von Guice.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface SocketWorkerFactory {

	/**
	 * Erstellt eine neue Instanz vom Typ Worker, der über einen Socket mit clients kommuniziert.
	 * 
	 * @param eine
	 *            Referenz zum SocketAgent (client)
	 * @return Instanz vom Worker
	 */
	Worker createSocketWorker(SocketAgent agent);

}
