package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AbstractClientSocketMapReduceTest;
import ch.zhaw.mapreduce.plugins.socket.TaskResult;

public class MapTaskRunnerTest extends AbstractClientSocketMapReduceTest {

	@Test
	public void shouldCallFailureFactoryMethodOnException() {
		MapTaskRunner mrt = new MapTaskRunner(mrtUuid, taskUuid, mapInstr, combInstr, mapInput, ctxFactory);
		final RuntimeException e = new RuntimeException();
		mockery.checking(new Expectations() {
			{
				oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
				will(returnValue(ctx));
				oneOf(mapInstr).map(ctx, mapInput);
				oneOf(ctx).getMapResult();
				oneOf(combInstr).combine(with(aNonNull(Iterator.class)));
				will(throwException(e));
			}
		});
		TaskResult res = mrt.runTask();
		assertEquals(mrtUuid, res.getMapReduceTaskUuid());
		assertEquals(taskUuid, res.getTaskUuid());
		assertTrue(res instanceof MapTaskResult);
		assertFalse(res.wasSuccessful());
		assertSame(e, res.getException());
	}

	@Test
	public void shouldCallSuccessFactoryMethodOnRegularCall() {
		MapTaskRunner mrt = new MapTaskRunner(mrtUuid, taskUuid, mapInstr, combInstr, mapInput, ctxFactory);
		mockery.checking(new Expectations() {
			{
				oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
				will(returnValue(ctx));
				oneOf(mapInstr).map(ctx, mapInput);
				oneOf(ctx).getMapResult();
				oneOf(combInstr).combine(with(aNonNull(Iterator.class)));
				will(returnValue(mapResult));
			}
		});
		TaskResult res = mrt.runTask();
		assertEquals(mrtUuid, res.getMapReduceTaskUuid());
		assertEquals(taskUuid, res.getTaskUuid());
		assertTrue(res instanceof MapTaskResult);
		assertTrue(res.wasSuccessful());
		assertSame(mapResult, ((MapTaskResult) res).getResult());
	}

	@Test
	public void shouldCallFailureFactoryMethodOnExceptionWitoutCombiner() {
		MapTaskRunner mrt = new MapTaskRunner(mrtUuid, taskUuid, mapInstr, null, mapInput, ctxFactory);
		mockery.checking(new Expectations() {
			{
				oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
				will(returnValue(ctx));
				oneOf(mapInstr).map(ctx, mapInput);
				oneOf(ctx).getMapResult();
				will(returnValue(mapResult));
			}
		});
		TaskResult res = mrt.runTask();
		assertEquals(mrtUuid, res.getMapReduceTaskUuid());
		assertEquals(taskUuid, res.getTaskUuid());
		assertTrue(res instanceof MapTaskResult);
		assertTrue(res.wasSuccessful());
		assertSame(mapResult, ((MapTaskResult) res).getResult());
	}

}
