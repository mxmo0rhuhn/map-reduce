package ch.zhaw.mapreduce.plugins.socket;

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
	 * @param Referenz
	 *            zum SocketResultCollector, da wo alle Resultate sind
	 * @return Instanz vom Worker
	 */
	SocketWorker createSocketWorker(SocketAgent agent, SocketResultCollector resultCollector);

}
