package ch.zhaw.mapreduce;

import com.google.inject.assistedinject.Assisted;

/**
 * Eine Implementation dieses Interface wird zum erstellen eines Context verwendet.
 * 
 * Die Idee von AssistedInject ist, dass man eine Factory macht, der einige Parameter übergeben werden und einige per
 * Binding dem zugehörigen Konstruktor injected werden. Also gibt es quasi ein Binding zwischen den Parametern der
 * Factory-Method und dem Konstruktor (also einer Implementation von Context, in diesem Fall). Da wir hier einen String
 * injecten, müssen wir die Assited Annotation mit dem zusätzlichen Parameter verwenden, da eine String nicht eindeutig
 * zugewisen werden kann. Die Annotation @Assisted muss aber für eindeutige Bindings nur im Konstruktor von der
 * Implementation des 'Ziel' Objekt angegeben werden, sodass guice weiss, welche Parameter von der Factory Methode
 * kommen.
 * 
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public interface ContextFactory {

	/**
	 * Erstellt eine neue Instanz eines Context mit den übergebenen Abhängigkeiten. Dank der Annotationen kann dieses
	 * Interface wunderbar mit AssistedInjection (guice extension) verwendet werden.
	 * 
	 * @param mapReduceUUID
	 *            globale Berechnugns ID / MapReduceTaskUUID
	 * @param taskUUID
	 *            taskUUID
	 * @return Instanz von Context
	 */
	Context createContext(@Assisted("mapReduceTaskUuid") String mapReduceUuid, @Assisted("taskUuid") String taskUuid);

}
