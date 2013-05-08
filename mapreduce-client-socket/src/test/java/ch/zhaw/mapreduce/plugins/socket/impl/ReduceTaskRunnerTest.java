package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertSame;

import java.util.Iterator;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AbstractClientSocketMapReduceTest;

public class ReduceTaskRunnerTest extends AbstractClientSocketMapReduceTest {
	
	
	@Test
	public void shouldCallFailureFactoryMethodOnException() {
		ReduceTaskRunner rtr = new ReduceTaskRunner(mrtUuid, taskUuid, redInstr, reduceKey, reduceValues, ctxFactory, strFactory);
		final RuntimeException e = new RuntimeException();
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(redInstr).reduce(with(ctx), with(reduceKey), with(aNonNull(Iterator.class))); will(throwException(e));
			oneOf(strFactory).createFailureResult(e); will(returnValue(stResult));
		}});
		assertSame(stResult, rtr.runTask());
	}
	
	@Test
	public void shouldCallSuccessFactoryMethodOnRegularCall() {
		ReduceTaskRunner rtr = new ReduceTaskRunner(mrtUuid, taskUuid, redInstr, reduceKey, reduceValues, ctxFactory, strFactory);
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(redInstr).reduce(with(ctx), with(reduceKey), with(aNonNull(Iterator.class))); 
			oneOf(ctx).getReduceResult(); will(returnValue(reduceResult));
			oneOf(strFactory).createSuccessResult(reduceResult); will(returnValue(stResult));
		}});
		assertSame(stResult, rtr.runTask());
	}
}
