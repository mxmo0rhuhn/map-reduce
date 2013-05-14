package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

import org.jmock.Expectations;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AbstractMapReduceMasterSocketTest;
import ch.zhaw.mapreduce.plugins.socket.ResultState;

public class SocketResultCollectorImplTest extends AbstractMapReduceMasterSocketTest {
	
	@Test
	public void shouldNotifySocketWorkerWithSuccess() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.requestedBy(srObserver));
		mockery.checking(new Expectations() {{ 
			atLeast(1).of(saRes).getMapReduceTaskUuid(); will(returnValue(mrUuid));
			atLeast(1).of(saRes).getTaskUuid(); will(returnValue(taskUuid));
			oneOf(saRes).getResult(); will(returnValue(Collections.emptyList()));
			atLeast(1).of(saRes).wasSuccessful(); will(returnValue(true));
			oneOf(srObserver).resultAvailable(mrUuid, taskUuid, true);
		}});
		coll.pushResult(saRes);
	}
	
	@Test
	public void shouldNotifySocketWorkerWithFailure() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.requestedBy(srObserver));
		mockery.checking(new Expectations() {{ 
			atLeast(1).of(saRes).getMapReduceTaskUuid(); will(returnValue(mrUuid));
			atLeast(1).of(saRes).getTaskUuid(); will(returnValue(taskUuid));
			atLeast(1).of(saRes).wasSuccessful(); will(returnValue(false));
			oneOf(saRes).getException(); will(returnValue(new Exception()));
			oneOf(srObserver).resultAvailable(mrUuid, taskUuid, false);
		}});
		coll.pushResult(saRes);
	}
	
	@Test
	public void shouldAddObserverToList() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.registerObserver(mrUuid, taskUuid, srObserver);
		ConcurrentMap<String, ResultState> results = coll.getResultStates();
		String key =coll.createKey(mrUuid, taskUuid);
		assertTrue(results.containsKey(key));
		assertTrue(results.get(key).requested());
		assertSame(results.get(key).requestedBy(), srObserver);
	}
	
	@Test
	public void shouldReturnSuccessIfResultIsAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.resultAvailable(true));
		assertTrue(coll.registerObserver(mrUuid, taskUuid, srObserver));
	}
	
	@Test
	public void shouldReturnFailureIfResultIsAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		coll.getResultStates().put(coll.createKey(mrUuid, taskUuid), ResultState.resultAvailable(true));
		assertTrue(coll.registerObserver(mrUuid, taskUuid, srObserver));
	}
	
	@Test
	public void shouldReturnNullIfResultIsNotAvailable() {
		SocketResultCollectorImpl coll = new SocketResultCollectorImpl(persistence);
		assertNull(coll.registerObserver(mrUuid, taskUuid, srObserver));
	}

}
