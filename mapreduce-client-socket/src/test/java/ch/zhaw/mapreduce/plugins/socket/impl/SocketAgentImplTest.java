package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jmock.Expectations;
import org.jmock.States;
import org.jmock.api.ThreadingPolicy;
import org.jmock.auto.Auto;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AbstractClientSocketMapReduceTest;
import ch.zhaw.mapreduce.plugins.socket.AgentTaskState;
import ch.zhaw.mapreduce.plugins.socket.AgentTaskState.State;

public class SocketAgentImplTest extends AbstractClientSocketMapReduceTest {

	private Synchroniser sync;

	@Auto
	private States pusherState;

	@Override
	protected ThreadingPolicy useThreadingPolicy() {
		this.sync = new Synchroniser();
		return sync;
	}

	@Test
	public void shouldSetClientIp() {
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory, execMock, execMock, resCollector, sarFactory,
				taskRunTimeout);
		assertEquals(clientIp, sa.getIp());
	}

	@Test
	public void shouldRunSmoothly() {
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory, execMock, execMock, resCollector, sarFactory,
				taskRunTimeout);
		sa.helloslave();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldOnlyAcceptOneTaskAtATime() throws Exception {
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory, execMock, execMock, resCollector, sarFactory,
				taskRunTimeout);
		mockery.checking(new Expectations() {
			{
				exactly(2).of(aTask).getMapReduceTaskUuid();
				exactly(2).of(aTask).getTaskUuid();
				exactly(2).of(trFactory).createTaskRunner(aTask);
				will(returnValue(taskRunner));
				exactly(2).of(execMock).submit(with(aNonNull(Callable.class)));
			}
		});
		AgentTaskState state1 = sa.runTask(aTask);
		assertTrue(state1.state() == State.ACCEPTED);
		AgentTaskState state2 = sa.runTask(aTask);
		assertTrue(state2.state() == State.REJECTED);
	}

	@Test
	public void shouldAcceptSecondTaskAfterFirstHasFinished() throws Exception {
		ExecutorService pusherExec = Executors.newSingleThreadExecutor();
		ExecutorService taskRunnerExec = Executors.newSingleThreadExecutor();
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory, taskRunnerExec, pusherExec, resCollector,
				sarFactory, taskRunTimeout);
		sa.startResultPusher();
		pusherState.startsAs("blockForResult");
		mockery.checking(new Expectations() {
			{
				oneOf(aTask).getMapReduceTaskUuid();
				will(returnValue(mrtUuid));
				oneOf(aTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(trFactory).createTaskRunner(aTask);
				will(returnValue(taskRunner));
				oneOf(taskRunner).runTask();
				will(returnValue(taskResult));
				oneOf(sarFactory).createFromTaskResult(mrtUuid, taskUuid, taskResult);
				will(returnValue(saResult));
				oneOf(resCollector).pushResult(saResult);
				then(pusherState.is("pushedResult"));
				inSequence(events);
				oneOf(aTask).getMapReduceTaskUuid();
				will(returnValue(mrtUuid));
				oneOf(aTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(trFactory).createTaskRunner(aTask);
				will(returnValue(taskRunner));
			}
		});
		AgentTaskState state1 = sa.runTask(aTask);
		assertTrue(state1.state() == State.ACCEPTED);
		sync.waitUntil(pusherState.is("pushedResult"), 200);
		Thread.yield(); // task wird grad nachher entfernt
		AgentTaskState state2 = sa.runTask(aTask);
		assertTrue(state2.state() == State.ACCEPTED);
	}

	@Test
	public void shouldPushResult() throws Exception {
		ExecutorService pusherExec = Executors.newSingleThreadExecutor();
		ExecutorService taskRunnerExec = Executors.newSingleThreadExecutor();
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory, taskRunnerExec, pusherExec, resCollector,
				sarFactory, taskRunTimeout);
		sa.startResultPusher();
		pusherState.startsAs("blockForResult");
		mockery.checking(new Expectations() {
			{
				oneOf(aTask).getMapReduceTaskUuid();
				will(returnValue(mrtUuid));
				oneOf(aTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(trFactory).createTaskRunner(aTask);
				will(returnValue(taskRunner));
				oneOf(taskRunner).runTask();
				will(returnValue(taskResult));
				oneOf(sarFactory).createFromTaskResult(mrtUuid, taskUuid, taskResult);
				will(returnValue(saResult));
				then(pusherState.is("createdResult"));
				oneOf(resCollector).pushResult(saResult);
				then(pusherState.is("pushedResult"));
			}
		});
		AgentTaskState state1 = sa.runTask(aTask);
		assertTrue(state1.state() == State.ACCEPTED);
		sync.waitUntil(pusherState.is("pushedResult"), 200);
	}

	@Test
	public void shouldWrapExceptionsInResults() throws Exception {
		ExecutorService pusherExec = Executors.newSingleThreadExecutor();
		ExecutorService taskRunnerExec = Executors.newSingleThreadExecutor();
		SocketAgentImpl sa = new SocketAgentImpl(clientIp, trFactory, taskRunnerExec, pusherExec, resCollector,
				sarFactory, taskRunTimeout);
		sa.startResultPusher();
		pusherState.startsAs("blockForResult");
		mockery.checking(new Expectations() {
			{
				oneOf(aTask).getMapReduceTaskUuid();
				will(returnValue(mrtUuid));
				oneOf(aTask).getTaskUuid();
				will(returnValue(taskUuid));
				oneOf(trFactory).createTaskRunner(aTask);
				will(returnValue(taskRunner));
				oneOf(taskRunner).runTask();
				will(throwException(new RuntimeException()));
				oneOf(sarFactory).createFromException(with(mrtUuid), with(taskUuid), with(aNonNull(Exception.class)));
				will(returnValue(saResult));
				then(pusherState.is("createdResult"));
				oneOf(resCollector).pushResult(saResult);
				then(pusherState.is("pushedResult"));
			}
		});
		AgentTaskState state1 = sa.runTask(aTask);
		assertTrue(state1.state() == State.ACCEPTED);
		sync.waitUntil(pusherState.is("pushedResult"), 200);
	}
}