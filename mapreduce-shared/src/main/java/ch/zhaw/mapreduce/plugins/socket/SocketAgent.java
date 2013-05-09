package ch.zhaw.mapreduce.plugins.socket;

/**
 * Der Socket Agent ist quasi der Client-Seitige Worker. Er führt Tasks aus und gibt das Resultat zurück an den Master.
 * Der SocketAdapter ist somit der verbindende Teil zwischen dem Server und Client von der Client-Seite. Er wird bei der
 * initialen Registrierung auf den Server gesandt und als Callback wird ein Task damit ausgeführt.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface SocketAgent {

	/**
	 * Wird vom Master aufgerufen, sobald er den Worker akzeptiert hat.
	 */
	void helloslave();

	/**
	 * Wird vom Master/SocketWorker aufgerufen, um einen Task auf dem Client/Worker auszuführen.
	 * 
	 * @param task
	 *            der auszuführende Task
	 * @return ob der task akzeptiert wurde oder nicht
	 */
	AgentTaskState runTask(AgentTask task) ;

	/**
	 * Liefer die IP vom Client/Worker
	 * 
	 * @return IP Adresse
	 */
	String getIp();

}
