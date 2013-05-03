package ch.zhaw.mapreduce.plugins;

import com.google.inject.Injector;

/**
 * Ein AgentPlugin aktiviert einen Agent auf dem Server. Es wird beim Server-Start aufgestartet und muss dann seinen
 * jeweiligen Worker eine Schnittstelle zur Registration bieten. Dieser serverseite Agent melden dann jeden Worker beim
 * Pool an.
 * 
 * Zum Beispiel könnte es ein SocketPlugin geben, welches die Ausführung von Tasks auf einem anderen Rechner ermöglicht.
 * Der SocketAgent würde dann eine Schnittstelle für einen Worker (Rechner) zur Verfügung stellen, sodass sich diese bei
 * ihm anmelden können. Sobald sich am Agent ein Worker (Rechner) anmelden, kann der Agent dies dem Pool mitteilen. Da
 * der Pool nur Instanzen von {@link Worker}n akzeptiert, wäre ein SocketWorker einfach ein Wrapper, der dann die Tasks
 * per RPC auf einem Rechner ausführt.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface AgentPlugin {

	void start(Injector injector) throws PluginException;

	void stop();

}
