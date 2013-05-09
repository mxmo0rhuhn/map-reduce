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

public class ReduceTaskRunnerTest extends AbstractClientSocketMapReduceTest {
	
	@Test
	public void shouldCallFailureFactoryMethodOnException() {
		ReduceTaskRunner rtr = new ReduceTaskRunner(mrtUuid, taskUuid, redInstr, reduceKey, reduceValues, ctxFactory);
		final RuntimeException e = new RuntimeException();
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(redInstr).reduce(with(ctx), with(reduceKey), with(aNonNull(Iterator.class))); will(throwException(e));
		}});
		TaskResult res = rtr.runTask();
		assertTrue(res instanceof ReduceTaskResult);
		ReduceTaskResult rres = (ReduceTaskResult) res;
		assertEquals(mrtUuid, rres.getMapReduceTaskUuid());
		assertEquals(taskUuid, rres.getTaskUuid());
		assertFalse(rres.wasSuccessful());
		assertSame(e, rres.getException());
	}
	
	@Test
	public void shouldCallSuccessFactoryMethodOnRegularCall() {
		ReduceTaskRunner rtr = new ReduceTaskRunner(mrtUuid, taskUuid, redInstr, reduceKey, reduceValues, ctxFactory);
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(redInstr).reduce(with(ctx), with(reduceKey), with(aNonNull(Iterator.class)));
			oneOf(ctx).getReduceResult(); will(returnValue(reduceResult));
		}});
		TaskResult res = rtr.runTask();
		assertTrue(res instanceof ReduceTaskResult);
		ReduceTaskResult rres = (ReduceTaskResult) res;
		assertTrue(rres.wasSuccessful());
		assertSame(reduceResult, rres.getResult());
	}
}
