package ch.zhaw.mapreduce;

import java.util.List;

import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

import com.google.inject.assistedinject.Assisted;

/**
 * Diese Factory wird zum erstellen von {@link MapWorkerTask}s und {@link ReduceWorkerTask}s benützt.
 * 
 * Für dieses Interface existiert keine Implemtnation, da sie über die Guice-Extension 'AssistedInject' verwendet wird.
 * 
 * @author Reto
 * 
 */
public interface WorkerTaskFactory {

	/**
	 * Erstellt eine neue Instanz vom MapWorkerTask mit den übergebenen Parametern. Wenn der Konstruktor der konkreten
	 * Implemtation mehr Parameter hat als hier angegeben, werden diese von Guice injected.
	 * 
	 * @param mapInstr
	 *            die zu verwendende MapInstruction
	 * @param combinerInstr
	 *            die zu verwendende CombinerInstruction
	 * @return eine neue Instanz eines MapWorkerTask
	 */
	MapWorkerTask createMapWorkerTask(MapInstruction mapInstruction,
									  CombinerInstruction combinerInstr,
									  @Assisted("input") String input);

	/**
	 * Erstellt eine neue Instanz vom ReduceWorkerTask mit den übergebenen Parametern. Wenn der Konstruktor der
	 * konkreten Implemtation mehr Parameter hat als hier angegeben, werden diese von Guice injected.
	 * 
	 * Da zwei Parameter vom Typ String sind, müssen diese in der @Assisted Annotation näher beschrieben werden. So
	 * werden Verwechslungen vermieden.
	 * 
	 * @param key
	 *            der Key, für den reduziert wird
	 * @param reduceInstr
	 *            die zu verwendenden ReduceInstruction
	 * @return eine neue Instanz eines ReduceWorkerTask
	 */
	ReduceWorkerTask createReduceWorkerTask( @Assisted("key") String key,
											@Assisted ReduceInstruction reduceInstr,
											@Assisted List<KeyValuePair> toDo);
}
