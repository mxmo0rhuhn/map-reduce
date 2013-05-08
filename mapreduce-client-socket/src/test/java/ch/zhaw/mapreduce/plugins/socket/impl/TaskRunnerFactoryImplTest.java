package ch.zhaw.mapreduce.plugins.socket.impl;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.ReduceInstruction;
import ch.zhaw.mapreduce.plugins.socket.AbstractClientSocketMapReduceTest;
import ch.zhaw.mapreduce.plugins.socket.InvalidAgentTaskException;

public class TaskRunnerFactoryImplTest extends AbstractClientSocketMapReduceTest {

	private final MapAgentTask mapAgentTaskWithCombiner = new MapAgentTask(mrtUuid, taskUuid, miName, mi, ciName, ci,
			mapInput);

	private final MapAgentTask mapAgentTaskWithoutCombiner = new MapAgentTask(mrtUuid, taskUuid, miName, mi, null,
			null, mapInput);

	private final ReduceAgentTask reduceAgentTask = new ReduceAgentTask(mrtUuid, taskUuid, riName, ri, reduceKey, reduceValues);

	@Test
	public void shouldCreateMapTaskRunnerForMapAgentTask() throws InvalidAgentTaskException {
		TaskRunnerFactoryImpl f = new TaskRunnerFactoryImpl(mtrFactory, rtrFactory);
		this.mockery.checking(new Expectations() {
			{
				oneOf(mtrFactory).createMapTaskRunner(with(mrtUuid), with(taskUuid),
						with(aNonNull(MapInstruction.class)), with(aNonNull(CombinerInstruction.class)), with(mapInput));
			}
		});
		f.createTaskRunner(mapAgentTaskWithCombiner);
	}

	@Test
	public void shouldCreateMapTaskRunnerForMapAgentWithoutCombinerTask() throws InvalidAgentTaskException {
		TaskRunnerFactoryImpl f = new TaskRunnerFactoryImpl(mtrFactory, rtrFactory);
		this.mockery.checking(new Expectations() {
			{
				oneOf(mtrFactory).createMapTaskRunner(with(mrtUuid), with(taskUuid),
						with(aNonNull(MapInstruction.class)), with(aNull(CombinerInstruction.class)), with(mapInput));
			}
		});
		f.createTaskRunner(mapAgentTaskWithoutCombiner);
	}

	@Test
	public void shouldCreateReduceTaskRunnerForMapAgentTask() throws InvalidAgentTaskException {
		TaskRunnerFactoryImpl f = new TaskRunnerFactoryImpl(mtrFactory, rtrFactory);
		this.mockery.checking(new Expectations() {
			{
				oneOf(rtrFactory).createReduceTaskRunner(with(mrtUuid), with(taskUuid),
						with(aNonNull(ReduceInstruction.class)), with(reduceKey), with(reduceValues));
			}
		});
		f.createTaskRunner(reduceAgentTask);
	}

	@Test(expected = InvalidAgentTaskException.class)
	public void shouldNotAcceptNullTask() throws InvalidAgentTaskException {
		TaskRunnerFactoryImpl f = new TaskRunnerFactoryImpl(mtrFactory, rtrFactory);
		f.createTaskRunner(null);
	}

	@Test(expected = InvalidAgentTaskException.class)
	public void shoudlThrowInvalidAgentTaskExceptionIfClassIsNotValid() throws InvalidAgentTaskException {
		TaskRunnerFactoryImpl f = new TaskRunnerFactoryImpl(mtrFactory, rtrFactory);
		f.loadClass("iDoNotExist", new byte[] { 1, 2, 3 }, TaskRunnerFactoryImpl.class);
	}


}
