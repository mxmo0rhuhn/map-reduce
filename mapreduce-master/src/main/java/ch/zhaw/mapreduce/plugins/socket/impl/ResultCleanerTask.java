package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;

import ch.zhaw.mapreduce.plugins.socket.ResultState;
import ch.zhaw.mapreduce.plugins.socket.SocketResultCollector;

/**
 * Der ResultCleanerTask räumt die Resultate vom SocketResultCollector periodisch auf. Genau genommen sind dies jedoch
 * keine Resultate, sondern jeweils ein Status, in dem eine Zustellung eines Resultates ist. Dies ist entweder Available
 * (der Agent hat das Resultat geschickt, aber der SocketWorker hats noch nicht akzeptiert) oder Requested (der
 * SocketWorker will das Resultat aber der SocketAgent hats noch nicht geschickt). Damit sich diese Einträge nicht
 * unendlich anhäufen wird periodisch aufgeräumt.
 * 
 * Dabei macht es einen Unterschied, ob das Resultat vom SocketWorker angefordert wurde oder das Resultat verfügbar ist
 * und der SocketWorker dies noch nicht akzeptiert hat. Wenn es nämlich verfügbar ist und der SocketWorker akzeptiert es
 * für eine lange Zeit nicht, dann ist es mit SocketWorker falsch. Wenn es hingegen vom SocketWorker requested wurde,
 * was sofort nach dem Senden an den Agent passiert, und das dauert zu lange, dann könnte es auch sein, dass es einfach
 * ein sehr langer Task ist.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public class ResultCleanerTask implements Runnable {

	private static final Logger LOG = Logger.getLogger(ResultCleanerTask.class.getName());

	/**
	 * Referenz auf die Liste mit den Resultat-Stati vom SocketResultCollector
	 */
	private final ConcurrentMap<String, ResultState> results;

	/**
	 * Der Service zum Ausführen dieses Task
	 */
	private final ScheduledExecutorService schedulerService;

	/**
	 * Nach wie viel Zeitl dieser Task wieder ausgeführt wird
	 */
	private final long schedulingDelay;

	/**
	 * Zeit in Millisekunden, nach der ein Resultat mit dem Status Available in der Liste bleibt
	 */
	private final long availableTimeToLive;

	/**
	 * Zeit in Millisekunden, nach der ein Resultat mit dem Status Requested in der Liste bleibt
	 */
	private final long requestedTimeToLive;

	@Inject
	ResultCleanerTask(SocketResultCollector resultCollector,
			@Named("SocketScheduler") ScheduledExecutorService schedulerService,
			@Named("SocketResultCleanupSchedulingDelay") long schedulingDelay,
			@Named("AvailableResultTimeToLive") long availableTimeToLive,
			@Named("RequestedResultTimeToLive") long requestedTimeToLive) {
		this.results = resultCollector.getResultStates();
		this.schedulerService = schedulerService;
		this.schedulingDelay = schedulingDelay;
		this.requestedTimeToLive = requestedTimeToLive;
		this.availableTimeToLive = availableTimeToLive;
	}

	/**
	 * Startet diesen Task, indem er dem Scheduler-Service übergeben wird.
	 */
	@PostConstruct
	public void start() {
		LOG.log(Level.INFO, "Scheduling {0} with fixed delay of {1} ms",
				new Object[] { ResultCleanerTask.class.getSimpleName(), this.schedulingDelay });
		this.schedulerService.scheduleWithFixedDelay(this, this.schedulingDelay, this.schedulingDelay,
				TimeUnit.MILLISECONDS);
	}

	@Override
	public void run() {
		try {
			Iterator<Entry<String, ResultState>> iter = results.entrySet().iterator();
			while (iter.hasNext()) {
				removeNextIfTooOld(iter);
			}
		} catch (NoSuchElementException stop) {
			// der iterator ist nicht zwingend thread-safe. deshalb könnte es passieren, dass wir zum
			// 'hasNext' call noch ein next element haben, aber wenn wir die next methode aufrufen, ist das
			// weg. dann würde eine NoSuchElementException geworfen werden und wir wüssten, dass wir alle
			// Elemente durchsucht haben.// der iterator ist nicht zwingend thread-safe. deshalb könnte es
			// passieren, dass wir zum 'hasNext' call noch ein next element haben, aber wenn wir die next
			// methode aufrufen, ist das weg. dann würde eine NoSuchElementException geworfen werden und wir
			// wüssten, dass wir alle Elemente durchsucht haben
			LOG.fine("NoSuchElementException while invoking next on the results-iterator. Stop Cleaning.");
			return;
		} catch (Exception e) {
			// wenn irgendeine exception auftritt fangen wir diese einfach mal und hoeren somit mit dem aufräumen auf.
			// wir hoffen halt, dass diese exception beim nächsten mal nicht mehr auftritt :)
			LOG.log(Level.SEVERE, "Caught Exception while cleaning Results", e);
		}

	}

	/**
	 * Entfernt das nächste Element vom Iterator, wenn es schon länger als die zulässige Zeit in der Sammlung ist.
	 */
	void removeNextIfTooOld(Iterator<Entry<String, ResultState>> iter) {
		Entry<String, ResultState> result = iter.next();
		LOG.entering(getClass().getName(), "removeNextIfTooOld", result);

		ResultState state = result.getValue();
		boolean survives = survives(state);

		if (survives) {
			if (LOG.isLoggable(Level.FINE)) {
				LOG.log(Level.FINE, "Result with Key {0} survives", new Object[] { result.getKey() });
			}
		} else {
			LOG.log(Level.FINE, "Result with Key {0} too old. Removing", new Object[] { result.getKey() });
			try {
				iter.remove();
			} catch (Exception e) {
				LOG.log(Level.WARNING, "Caught Exception while removing old Result from Results", e);
			}
		}
		LOG.exiting(getClass().getName(), "removeNextIfTooOld");
	}

	/**
	 * Ob der Eintrag überlebt
	 */
	boolean survives(ResultState state) {
		long now = System.currentTimeMillis();
		long created = state.created();
		long ttl;
		if (state.requested()) {
			ttl = this.requestedTimeToLive;
		} else if (state.available()) {
			ttl = this.availableTimeToLive;
		} else {
			throw new IllegalArgumentException("Unknown State: " + state);
		}
		return now - created < ttl;
	}
}
