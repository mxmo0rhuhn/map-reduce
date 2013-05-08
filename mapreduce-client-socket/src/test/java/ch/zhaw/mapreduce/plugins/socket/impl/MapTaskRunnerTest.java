package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertSame;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.plugins.socket.AbstractClientSocketMapReduceTest;

public class MapTaskRunnerTest extends AbstractClientSocketMapReduceTest {
	
	@Test
	public void shouldCallFailureFactoryMethodOnException() {
		MapTaskRunner mrt = new MapTaskRunner(mrtUuid, taskUuid, mapInstr, combInstr, mapInput, ctxFactory, strFactory);
		final RuntimeException e = new RuntimeException();
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(mapInstr).map(ctx, mapInput);
			oneOf(ctx).getMapResult();
			oneOf(combInstr).combine(with(aNonNull(Iterator.class))); will(throwException(e));
			oneOf(strFactory).createFailureResult(e); will(returnValue(stResult));
		}});
		assertSame(stResult, mrt.runTask());
	}
	
	@Test
	public void shouldCallSuccessFactoryMethodOnRegularCall() {
		MapTaskRunner mrt = new MapTaskRunner(mrtUuid, taskUuid, mapInstr, combInstr, mapInput, ctxFactory, strFactory);
		final List<KeyValuePair> mapResult2 = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key2", "val2")});
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(mapInstr).map(ctx, mapInput);
			oneOf(ctx).getMapResult(); will(returnValue(mapResult));
			oneOf(combInstr).combine(with(aNonNull(Iterator.class))); will(returnValue(mapResult2));
			oneOf(strFactory).createSuccessResult(mapResult2); will(returnValue(stResult));
		}});
		assertSame(stResult, mrt.runTask());
	}
	
	@Test
	public void shouldCallFailureFactoryMethodOnExceptionWitoutCombiner() {
		MapTaskRunner mrt = new MapTaskRunner(mrtUuid, taskUuid, mapInstr, null, mapInput, ctxFactory, strFactory);
		final RuntimeException e = new RuntimeException();
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(mapInstr).map(ctx, mapInput); will(throwException(e));
			oneOf(strFactory).createFailureResult(e); will(returnValue(stResult));
		}});
		assertSame(stResult, mrt.runTask());
	}
	
	@Test
	public void shouldCallSuccessFactoryMethodOnRegularCallWitoutCombiner() {
		MapTaskRunner mrt = new MapTaskRunner(mrtUuid, taskUuid, mapInstr, null, mapInput, ctxFactory, strFactory);
		mockery.checking(new Expectations() {{ 
			oneOf(ctxFactory).createContext(mrtUuid, taskUuid);
			will(returnValue(ctx));
			oneOf(mapInstr).map(ctx, mapInput);
			oneOf(ctx).getMapResult(); will(returnValue(mapResult));
			oneOf(strFactory).createSuccessResult(mapResult); will(returnValue(stResult));
		}});
		assertSame(stResult, mrt.runTask());
	}

}
