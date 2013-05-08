package ch.zhaw.mapreduce.plugins.socket.impl;

import javax.inject.Inject;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.plugins.socket.AgentTask;
import ch.zhaw.mapreduce.plugins.socket.ByteArrayClassLoader;
import ch.zhaw.mapreduce.plugins.socket.InvalidAgentTaskException;
import ch.zhaw.mapreduce.plugins.socket.MapTaskRunnerFactory;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;
import ch.zhaw.mapreduce.plugins.socket.TaskRunnerFactory;

public final class TaskRunnerFactoryImpl implements TaskRunnerFactory {

	private static final ByteArrayClassLoader CLASSlOADER = new ByteArrayClassLoader();

	private final MapTaskRunnerFactory mtrFactory;

	@Inject
	TaskRunnerFactoryImpl(MapTaskRunnerFactory mtrFactory) {
		this.mtrFactory = mtrFactory;
	}

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
		} else {
			throw new UnsupportedOperationException("implement me");
		}
	}

	<T> T loadClass(String mapReduceTaskUuid, String className, byte[] bytes, Class<T> type) throws InvalidAgentTaskException {
		try {
			Class<?> klass = CLASSlOADER.defineClass(mapReduceTaskUuid, className, bytes);
			return (T) klass.newInstance();
		} catch (Throwable e) {
			// throwable weil classloading auch errors schmeisst. wir muessen alles fangen, sonst verstrickt sich SIMON
			throw new InvalidAgentTaskException(e);
		}
	}

}
