package ch.zhaw.mapreduce.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.Pool;

public class PoolStatisticsPrinter implements Runnable {

	private static final Logger LOG = Logger.getLogger(PoolStatisticsPrinter.class.getName());

	private final Pool pool;

	private final ScheduledExecutorService scheduler;

	private final long timeout;

	@Inject
	PoolStatisticsPrinter(Pool pool, @Named("supervisorScheduler") ScheduledExecutorService scheduler,
			@Named("statisticsPrinterTimeout") long statisticsTimeout) {
		this.pool = pool;
		this.scheduler = scheduler;
		this.timeout = statisticsTimeout;
	}

	@PostConstruct
	public void start() {
		this.scheduler.scheduleWithFixedDelay(this, timeout, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public void run() {
		int poolSize = pool.getCurrentPoolSize();
		int freeWorkers = pool.getFreeWorkers();
		int enqueuedTasks = pool.enqueuedTasks();
		long runTasks = pool.totalRunTasks();
		LOG.log(Level.INFO, "Pool: Worker: {0} known Worker, {1} free Worker, {2} Tasks enqueued, {3} Tasks run", new Object[] {
				poolSize, freeWorkers, enqueuedTasks, runTasks });
	}

}
