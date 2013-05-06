package ch.zhaw.mapreduce.plugins.socket;

/**
 * Factory um neue SocketAgents zu erstellen. Die Implementation basiert auf AssistedInject.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface SocketAgentFactory {

	/**
	 * Erstellt eine neue Instanz von einem SocketAgent mit der spezifiziert clientIp.
	 * 
	 * @param clientIp
	 *            IP adresse vom socket client
	 * @return neue Instanz vom SocketAgent
	 */
	SocketAgent createSocketAgent(String clientIp);

}
