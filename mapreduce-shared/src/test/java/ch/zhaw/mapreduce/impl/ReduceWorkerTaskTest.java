package ch.zhaw.mapreduce.impl;


public class ReduceWorkerTaskTest {
	
	// TODO rewrite test without pool and plugins
//
//	@Rule
//	public JUnitRuleMockery mockery = new JUnitRuleMockery() {
//		{
//			setThreadingPolicy(new Synchroniser());
//		}
//	};
//
//	@Auto
//	private Sequence events;
//
//	@Mock
//	private ReduceInstruction reduceInstr;
//
//	@Mock
//	private Context ctx;
//
//	@Mock
//	private ContextFactory ctxFactory;
//
//	@Mock
//	private Worker worker;
//
//	private final String taskUUID = "taskUUID";
//	
//	private final String key = "key";
//
//	private final List<KeyValuePair> keyVals = Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1"), new KeyValuePair("key2", "val2")});
//
//	@Test
//	public void shouldSetMapReduceTaskUUID() {
//		ReduceWorkerTask task = new ReduceWorkerTask("uuid", reduceInstr, key, keyVals, taskUUID);
//		assertEquals("uuid", task.getMapReduceTaskUuid());
//	}
//
//	@Test
//	public void shouldSetReduceInstruction() {
//		ReduceWorkerTask task = new ReduceWorkerTask("uuid", reduceInstr, key, keyVals, taskUUID);
//		assertSame(reduceInstr, task.getReduceInstruction());
//	}
//
//	@Test
//	public void shouldRunReduceInstruction() {
//		Executor poolExec = Executors.newSingleThreadExecutor();
//		Pool pool = new Pool(poolExec);
//		pool.init();
//		final ReduceWorkerTask task = new ReduceWorkerTask("uuid", reduceInstr, key, keyVals, taskUUID);
//		this.mockery.checking(new Expectations() {
//			{
//				oneOf(reduceInstr).reduce(with(ctx), with(key), with(aNonNull(Iterator.class)));
//			}
//		});
//		task.runTask(ctx);
//	}
//
//	@Test
//	public void shouldSetInputUUID() {
//		ReduceWorkerTask task = new ReduceWorkerTask("uuid", reduceInstr, key, keyVals, taskUUID);
//		assertEquals(taskUUID, task.getTaskUuid());
//	}
//
//	@Test
//	public void shouldSetStateToFailedOnException() {
//		ReduceWorkerTask task = new ReduceWorkerTask("uuid", reduceInstr, key, keyVals, taskUUID);
//		this.mockery.checking(new Expectations() {
//			{
//				oneOf(reduceInstr).reduce(with(ctx), with(key), with(aNonNull(Iterator.class)));
//				will(throwException(new NullPointerException()));
//				oneOf(worker).cleanSpecificResult("uuid", taskUUID);
//			}
//		});
//		task.setWorker(worker);
//		task.runTask(ctx);
//		assertEquals(State.FAILED, task.getCurrentState());
//		assertSame(worker, task.getWorker());
//	}
//
//	@Test
//	public void shouldSetStateToCompletedOnSuccess() {
//		ReduceWorkerTask task = new ReduceWorkerTask("uuid", reduceInstr, key, keyVals, taskUUID);
//		this.mockery.checking(new Expectations() {
//			{
//				oneOf(reduceInstr).reduce(with(ctx), with(key), with(aNonNull(Iterator.class)));
//			}
//		});
//		task.setWorker(worker);
//		task.runTask(ctx);
//		assertEquals(State.COMPLETED, task.getCurrentState());
//	}
//
//	@Test
//	public void shouldSetStateToInitiatedInitially() {
//		ReduceWorkerTask task = new ReduceWorkerTask("uuid", reduceInstr, key, keyVals, taskUUID);
//		assertEquals(State.INITIATED, task.getCurrentState());
//	}
//
//	@Test
//	public void shouldBeInProgressWhileRunning() throws InterruptedException, BrokenBarrierException {
//		ExecutorService poolExec = Executors.newSingleThreadExecutor();
//		final CyclicBarrier barrier = new CyclicBarrier(2);
//		ExecutorService taskExec = Executors.newSingleThreadExecutor();
//		final Pool pool = new Pool(poolExec);
//		pool.init();
//		ThreadWorker worker = new ThreadWorker(pool, taskExec, ctxFactory);
//		pool.donateWorker(worker);
//		final ReduceWorkerTask task = new ReduceWorkerTask("mrtUuid",  new ReduceInstruction() {
//			@Override
//			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
//				try {
//					barrier.await();
//				} catch (Exception e) {
//					throw new IllegalStateException(e);
//				}
//			}
//
//		}, key, keyVals, taskUUID);
//		this.mockery.checking(new Expectations() {
//			{
//				oneOf(ctxFactory).createContext("mrtUuid", taskUUID);
//				will(returnValue(ctx));
//			}
//		});
//		pool.enqueueWork(task);
//		Thread.yield();
//		Thread.sleep(200);
//		assertEquals(State.INPROGRESS, task.getCurrentState());
//		try {
//			barrier.await(100, TimeUnit.MILLISECONDS);
//		} catch (TimeoutException te) {
//			fail("should return immediately");
//		}
//	}
//
//	@Test
//	public void shouldBeAbleToRerunTests() {
//		ExecutorService poolExec = Executors.newSingleThreadExecutor();
//		ExactCommandExecutor threadExec1 = new ExactCommandExecutor(1);
//		ExactCommandExecutor threadExec2 = new ExactCommandExecutor(1);
//		final Pool pool = new Pool(poolExec);
//		final AtomicInteger cnt = new AtomicInteger();
//		pool.init();
//		ThreadWorker worker1 = new ThreadWorker(pool, threadExec1, ctxFactory);
//		pool.donateWorker(worker1);
//		ThreadWorker worker2 = new ThreadWorker(pool, threadExec2, ctxFactory);
//		pool.donateWorker(worker2);
//		final ReduceWorkerTask task = new ReduceWorkerTask("mrtUuid", new ReduceInstruction() {
//			
//			@Override
//			public void reduce(ReduceEmitter emitter, String key, Iterator<KeyValuePair> values) {
//				if (cnt.get() == 0) {
//					cnt.incrementAndGet();
//					throw new NullPointerException();
//				} else if (cnt.get() == 1) {
//					// successful
//				} else {
//					throw new NullPointerException();
//				}
//			}
//		}, key, keyVals, taskUUID);
//		this.mockery.checking(new Expectations() {
//			{
//				oneOf(ctxFactory).createContext("mrtUuid", taskUUID);
//				will(returnValue(ctx));
//				inSequence(events);
//				oneOf(ctx).destroy();
//				inSequence(events);
//				oneOf(ctxFactory).createContext("mrtUuid", taskUUID);
//				will(returnValue(ctx));
//			}
//		});
//		pool.enqueueWork(task);
//		assertTrue(threadExec1.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
//		assertEquals(State.FAILED, task.getCurrentState());
//		assertSame(worker1, task.getWorker());
//		pool.enqueueWork(task);
//		assertTrue(threadExec2.waitForExpectedTasks(100, TimeUnit.MILLISECONDS));
//		assertEquals(State.COMPLETED, task.getCurrentState());
//		assertSame(worker2, task.getWorker());
//	}
//
//	@Test
//	public void shouldBeEnqueuedAfterSubmissionToPool() {
//		Pool pool = new Pool(Executors.newSingleThreadExecutor());
//		pool.init();
//		final ReduceWorkerTask task = new ReduceWorkerTask("mrtuid", reduceInstr, key, keyVals, taskUUID);
//		this.mockery.checking(new Expectations() {
//			{
//				never(reduceInstr);
//			}
//		});
//		pool.enqueueWork(task);
//		assertEquals(State.ENQUEUED, task.getCurrentState());
//	}

}