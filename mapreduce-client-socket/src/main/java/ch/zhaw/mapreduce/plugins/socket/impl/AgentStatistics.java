package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * Diese Klasse sammelt Statistiken von einem Agenten und loggt diese periodisch.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class AgentStatistics implements Runnable {

	private static final Logger LOG = Logger.getLogger(AgentStatistics.class.getName());

	/**
	 * Anzahl Tasks, die zur Ausführung akzeptiert wurden
	 */
	private final AtomicLong acceptedTasks = new AtomicLong();

	/**
	 * Anzahl Tasks, die vom Agenten abgewisen wurden.
	 */
	private final AtomicLong rejectedTasks = new AtomicLong();

	/**
	 * Anzahl Tasks, die vom Agenten erfolgreich abgeschlossen worden sind.
	 */
	private final AtomicLong successfullyCompletedTasks = new AtomicLong();

	/**
	 * Anzahl Tasks, die auf dem Agent fehlgeschlagen sind.
	 */
	private final AtomicLong failedTasks = new AtomicLong();

	private final ScheduledExecutorService scheduler;

	private final long statisticsTimeout;

	@Inject
	AgentStatistics(@Named("SocketScheduler") ScheduledExecutorService scheduler,
			@Named("StatisticsPrinterTimeout") long statisticsTimeout) {
		this.scheduler = scheduler;
		this.statisticsTimeout = statisticsTimeout;
	}

	@PostConstruct
	public void startPrinter() {
		LOG.log(Level.INFO, "Starting {0} with delay of {1} ms", new Object[] { getClass().getName(),
				this.statisticsTimeout });
		this.scheduler.scheduleWithFixedDelay(this, this.statisticsTimeout, this.statisticsTimeout,
				TimeUnit.MILLISECONDS);
	}

	public void acceptedTask() {
		this.acceptedTasks.incrementAndGet();
	}

	public void rejectTask() {
		this.rejectedTasks.incrementAndGet();
	}

	public void successfulTask() {
		this.successfullyCompletedTasks.incrementAndGet();
	}

	public void failedTask() {
		this.failedTasks.incrementAndGet();
	}

	@Override
	public void run() {
		long totalRun = successfullyCompletedTasks.longValue() + failedTasks.longValue();
		long successrate = 100 / totalRun * successfullyCompletedTasks.longValue();
		LOG.log(Level.INFO, "Statistics: {0} Accepted Tasks, {1} Rejected Tasks, {2} Run Tasks ({3} successful)",
				new Object[] { acceptedTasks.longValue(), rejectedTasks.longValue(), totalRun, successrate });
	}

}
