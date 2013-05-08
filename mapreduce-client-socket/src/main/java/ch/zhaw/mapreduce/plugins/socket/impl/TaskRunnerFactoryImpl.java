package ch.zhaw.mapreduce.plugins.socket.impl;

import javax.inject.Inject;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.plugins.socket.AgentTask;
import ch.zhaw.mapreduce.plugins.socket.ByteArrayClassLoader;
import ch.zhaw.mapreduce.plugins.socket.InvalidAgentTaskException;
import ch.zhaw.mapreduce.plugins.socket.MapTaskRunnerFactory;
import ch.zhaw.mapreduce.plugins.socket.ReduceTaskRunnerFactory;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;
import ch.zhaw.mapreduce.plugins.socket.TaskRunnerFactory;

/**
 * Erstellt neue TaskRunners, welche Tasks ausfuehren koennen. Diese Implementation kann nicht durch AssistedInject
 * ersetzt werden, weil die einzelnen Instruktionen manuell gewandelt werden muessen und je nach dem, ob es ein Map oder
 * Reduce Task ist, werden andere Getter-Methoden verwendet.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class TaskRunnerFactoryImpl implements TaskRunnerFactory {

	/**
	 * Klassenlader, der Klassen aufgrund ihrere Byte-Codes laden kann.
	 */
	private final ByteArrayClassLoader classLoader = new ByteArrayClassLoader();

	/**
	 * Factory zum erstellen der MapTaskRunner
	 */
	private final MapTaskRunnerFactory mtrFactory;

	/**
	 * Factory zum erstellen der ReduceTaskRunner
	 */
	private final ReduceTaskRunnerFactory rtrFactory;

	@Inject
	TaskRunnerFactoryImpl(MapTaskRunnerFactory mtrFactory, ReduceTaskRunnerFactory rtrFactory) {
		this.mtrFactory = mtrFactory;
		this.rtrFactory = rtrFactory;
	}

	/**
	 * Erstellt einen neuen TaskRunner basierend auf dem AgentTask.
	 * 
	 * @param AgentTask
	 *            existierender AgentTask, 'frisch' vom Netz
	 * @return TaskRunner, der einen Task ausführen kann
	 */
	@Override
	public TaskRunner createTaskRunner(AgentTask task) throws InvalidAgentTaskException {
		if (task instanceof MapAgentTask) {
			MapAgentTask mt = (MapAgentTask) task;
			MapInstruction mapInstr = loadClass(mt.getMapInstructionName(), mt.getMapInstruction(),
					MapInstruction.class);
			CombinerInstruction combInstr = null;
			if (mt.getCombinerInstructionName() != null) {
				combInstr = loadClass(mt.getCombinerInstructionName(), mt.getCombinerInstruction(),
						CombinerInstruction.class);
			}
			return this.mtrFactory.createMapTaskRunner(mt.getMapReduceTaskUuid(), mt.getTaskUuid(), mapInstr,
					combInstr, mt.getInput());
		} else if (task instanceof ReduceAgentTask) {
			ReduceAgentTask rt = (ReduceAgentTask) task;
			ReduceInstruction redInstr = loadClass(rt.getReduceInstructionName(), rt.getReduceInstruction(),
					ReduceInstruction.class);
			return this.rtrFactory.createReduceTaskRunner(rt.getMapReduceTaskUuid(), rt.getTaskUuid(), redInstr,
					rt.getKey(), rt.getValues());
		} else {
			throw new InvalidAgentTaskException("Cannot Handle: " + task);
		}
	}

	/**
	 * Lädt die Klasse mit dem gegebenen Namen mit dem ByteArrayClassLoader.
	 * 
	 * @param className
	 *            Name der Implementation
	 * @param bytes
	 *            Bytes der Implementation
	 * @param type
	 *            Interface-Typ für Typisierung
	 * @return neue Instanz der Implementation
	 * @throws InvalidAgentTaskException
	 *             wenn die klasse nicht geladen werden kann
	 */
	<T> T loadClass(String className, byte[] bytes, Class<T> type) throws InvalidAgentTaskException {
		try {
			Class<?> klass = classLoader.defineClass(className, bytes);
			return (T) klass.newInstance();
		} catch (Throwable e) {
			// throwable weil classloading auch errors schmeisst. wir muessen alles fangen, sonst verstrickt sich SIMON
			throw new InvalidAgentTaskException(e);
		}
	}

}
