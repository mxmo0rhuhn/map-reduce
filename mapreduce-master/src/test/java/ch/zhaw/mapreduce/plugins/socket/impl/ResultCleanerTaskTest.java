package ch.zhaw.mapreduce.plugins.socket.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map.Entry;

import org.jmock.Expectations;
import org.jmock.auto.Mock;
import org.junit.Test;

import ch.zhaw.mapreduce.plugins.socket.AbstractMapReduceMasterSocketTest;
import ch.zhaw.mapreduce.plugins.socket.ResultState;

public class ResultCleanerTaskTest extends AbstractMapReduceMasterSocketTest {
	
	@Mock
	Iterator<Entry<String, ResultState>> iter;
	
	@Mock
	Entry<String, ResultState> entry;
	
	@Test
	public void shouldRemoveEntry() {
		final ResultState state = ResultState.resultAvailable(true);
		mockery.checking(new Expectations() {{ 
			oneOf(resCollector).getResultStates();
			oneOf(iter).next(); will(returnValue(entry));
			oneOf(entry).getValue(); will(returnValue(state));
			allowing(entry).getKey();
			oneOf(iter).remove();
		}});
		int availableTtl = 1;
		int requestedTtl = 1;
		ResultCleanerTask cleaner = new ResultCleanerTask(resCollector, schedService, 1, availableTtl, requestedTtl);
		cleaner.removeNextIfTooOld(iter);
	}
	
	@Test
	public void shouldNotRemoveEntry() {
		final ResultState state = ResultState.resultAvailable(true);
		mockery.checking(new Expectations() {{ 
			oneOf(resCollector).getResultStates();
			oneOf(iter).next(); will(returnValue(entry));
			oneOf(entry).getValue(); will(returnValue(state));
			allowing(entry).getKey();
		}});
		int availableTtl = 10000;
		int requestedTtl = 1;
		ResultCleanerTask cleaner = new ResultCleanerTask(resCollector, schedService, 1, availableTtl, requestedTtl);
		cleaner.removeNextIfTooOld(iter);
	}
	
	@Test
	public void shouldLetNewTasksSurviveAvailable() {
		mockery.checking(new Expectations() {{ 
			oneOf(resCollector).getResultStates();
		}});
		int availableTtl = 1000;
		int requestedTtl = 1;
		ResultCleanerTask cleaner = new ResultCleanerTask(resCollector, schedService, 1, availableTtl, requestedTtl);
		ResultState state = ResultState.resultAvailable(true);
		assertTrue(cleaner.survives(state));
	}
	
	@Test
	public void shouldNotLetOldTasksSurviveAvailable() throws Exception {
		mockery.checking(new Expectations() {{ 
			oneOf(resCollector).getResultStates();
		}});
		int availableTtl = 1;
		int requestedTtl = 1000;
		ResultCleanerTask cleaner = new ResultCleanerTask(resCollector, schedService, 1, availableTtl, requestedTtl);
		ResultState state = ResultState.resultAvailable(true);
		Thread.sleep(10);
		assertFalse(cleaner.survives(state));
	}

	@Test
	public void shouldLetNewTasksSurviveRequested() {
		mockery.checking(new Expectations() {{ 
			oneOf(resCollector).getResultStates();
		}});
		int availableTtl = 1;
		int requestedTtl = 1000;
		ResultCleanerTask cleaner = new ResultCleanerTask(resCollector, schedService, 1, availableTtl, requestedTtl);
		ResultState state = ResultState.requestedBy(srObserver);
		assertTrue(cleaner.survives(state));
	}
	
	@Test
	public void shouldNotLetOldTasksSurviveRequested() throws Exception {
		mockery.checking(new Expectations() {{ 
			oneOf(resCollector).getResultStates();
		}});
		int availableTtl = 1000;
		int requestedTtl = 1;
		ResultCleanerTask cleaner = new ResultCleanerTask(resCollector, schedService, 1, availableTtl, requestedTtl);
		ResultState state = ResultState.requestedBy(srObserver);
		Thread.sleep(10);
		assertFalse(cleaner.survives(state));
	}

}
