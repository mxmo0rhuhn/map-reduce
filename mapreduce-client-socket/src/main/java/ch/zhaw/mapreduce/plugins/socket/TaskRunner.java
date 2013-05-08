package ch.zhaw.mapreduce.plugins.socket;

/**
 * Ein TaskRunner ist ein einfacher Runner, welcher eine Instruction zusammen mit dem Input ausführt und das Resultat in
 * den entsprecheden Typen wandelt. Ausserdem muss sich der Runner um Exceptions kümmern, d.h. diese auch korrekt in die
 * Resultat-Struktur verpacken.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface TaskRunner {

	/**
	 * Führt den Task aus und füllt das Resultat in ein SocketTaskResult. Darin muss auch die Exception, falls eine
	 * geworfen wurde.
	 * 
	 * @return eine Instanz vom SocketTaskResult. Darf nicht null sein - im Fehlerfall muss die Exception Teil des
	 *         Resultats sein.
	 */
	SocketTaskResult runTask();

}
