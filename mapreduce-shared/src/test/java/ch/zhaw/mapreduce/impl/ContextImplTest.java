package ch.zhaw.mapreduce.impl;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class ContextImplTest {
	
	@Test
	public void shouldNeverReturnNull() {
		ContextImpl ctx = new ContextImpl();
		assertNotNull(ctx.getMapResult());
		assertNotNull(ctx.getReduceResult());
	}

}
