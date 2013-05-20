package ch.zhaw.mapreduce;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;
import ch.zhaw.mapreduce.plugins.socket.impl.NamedThreadFactory;

public final class Master {

	private static final Logger LOG = Logger.getLogger(Master.class.getName());

	private final ExecutorService executorPool = Executors.newSingleThreadExecutor(new NamedThreadFactory(
			"AfterShuffleHookService"));

	private final Shuffler shuffler;

	private final Provider<Persistence> persistenceProvider;

	private final Pool pool;

	private final WorkerTaskFactory workerTaskFactory;

	private final int maxrunningtasks;

	@Inject
	Master(Pool pool, WorkerTaskFactory workerTaskFactory, Shuffler shuffler,
			Provider<Persistence> persistenceProvider, @Named("MaxRunningTasks") int maxrunningtasks) {
		this.pool = pool;
		this.workerTaskFactory = workerTaskFactory;
		this.shuffler = shuffler;
		this.persistenceProvider = persistenceProvider;
		this.maxrunningtasks = maxrunningtasks;
	}

	public Map<String, List<String>> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction, final ReduceInstruction reduceInstruction,
			ShuffleProcessorFactory afterShuffleHook, Iterator<String> inputs) throws InterruptedException {

		// für sämtliche tasks einer berechnung muss die gleiche persistence verwendet werden!
		Persistence pers = this.persistenceProvider.get();

		LOG.info("Start Running Map Tasks");
		runMapTasks(mapInstruction, combinerInstruction, inputs, pers);
		LOG.info("Done Running Map Tasks");

		LOG.info("Retrieve all Map Results from Persistence");
		List<KeyValuePair> mapResults = pers.getMapResults();

		LOG.log(Level.INFO, "Shuffle all {0} Map Results", mapResults.size());
		Map<String, List<KeyValuePair>> shuffledResults = shuffler.shuffle(mapResults);
		LOG.info("Done shuffling Map Results");

		if (afterShuffleHook != null) {
			LOG.info("Running AfterShuffleHook with Exector");
			executorPool.execute(afterShuffleHook.getNewRunnable(shuffledResults.entrySet().iterator()));
		} else {
			LOG.info("No AfterShuffleHook");
		}

		LOG.info("Start Running Reduce Tasks");
		runReduceTasks(reduceInstruction, shuffledResults, pers);
		LOG.info("Done Running Reduce Tasks");

		LOG.info("Retrieve all Reduce Results from Persistence");
		Map<String, List<String>> results = pers.getReduceResults();

		LOG.info("Kill Persistence");
		pers.suicide();

		return results;
	}

	/* Methoden fuer Map-Phase */

	private void runMapTasks(MapInstruction mapInstruction, CombinerInstruction combinerInstruction,
			Iterator<String> inputs, Persistence pers) throws InterruptedException {
		List<MapWorkerTask> runningTasks = new LinkedList<MapWorkerTask>();
		while (inputs.hasNext()) {
			while (runningTasks.size() > maxrunningtasks) {
				if (!housekeepingMap(runningTasks)) {
					Thread.sleep(200);
				}
			}
			String input = inputs.next();
			MapWorkerTask task = this.workerTaskFactory.createMapWorkerTask(mapInstruction, combinerInstruction, input,
					pers);
			this.pool.enqueueTask(task);
			runningTasks.add(task);
		}

		// warten, bis alle ausgeführt worden sind
		while (!runningTasks.isEmpty()) {
			if (!housekeepingMap(runningTasks)) {
				Thread.sleep(200); // nur warten, wenn sich nichts getan hat
			}
		}
	}

	private boolean housekeepingMap(List<MapWorkerTask> runningTasks) throws InterruptedException {
		List<MapWorkerTask> done = new LinkedList<MapWorkerTask>();
		List<MapWorkerTask> failed = new LinkedList<MapWorkerTask>();
		for (MapWorkerTask task : runningTasks) {
			switch (task.getCurrentState()) {
			case COMPLETED:
				done.add(task);
				break;
			case FAILED:
				failed.add(task);
				break;
			case ENQUEUED: // fall through
			case INITIATED: // fall through
			case INPROGRESS:
				// ok, wird demnaechst ausgefuehrt
				break;
			case ABORTED:
				throw new IllegalStateException("Task must not be in this list when in State ABORTED");
			default:
				throw new IllegalStateException("Unhandled state: " + task.getCurrentState());
			}
		}
		LOG.log(Level.FINE, "{0} Tasks done, {1} Tasks failed", new Object[] { done.size(), failed.size() });
		runningTasks.removeAll(done);
		runningTasks.addAll(restartFailedMap(failed));
		return done.isEmpty() && failed.isEmpty();
	}

	private List<MapWorkerTask> restartFailedMap(List<MapWorkerTask> faileds) throws InterruptedException {
		LOG.entering(getClass().getName(), "restartFailedMap", faileds.size());
		List<MapWorkerTask> newtasks = new ArrayList<MapWorkerTask>(faileds.size());
		for (MapWorkerTask failed : faileds) {
			MapWorkerTask newtask = this.workerTaskFactory.createMapWorkerTask(failed.getMapInstruction(),
					failed.getCombinerInstruction(), failed.getInput(), failed.getPersistence());
			this.pool.enqueueTask(newtask);
			newtasks.add(newtask);
		}
		return newtasks;
	}

	/* Methoden fuer Reduce-Phase */

	private void runReduceTasks(ReduceInstruction redInstruction, Map<String, List<KeyValuePair>> shuffled,
			Persistence pers) throws InterruptedException {

		List<ReduceWorkerTask> runningTasks = new LinkedList<ReduceWorkerTask>();
		for (Entry<String, List<KeyValuePair>> entry : shuffled.entrySet()) {
			while (runningTasks.size() > maxrunningtasks) {
				if (!housekeepingReduce(runningTasks)) {
					Thread.sleep(200);
				}
			}
			String key = entry.getKey();
			List<KeyValuePair> values = entry.getValue();
			ReduceWorkerTask task = this.workerTaskFactory.createReduceWorkerTask(redInstruction, key, values, pers);
			this.pool.enqueueTask(task);
			runningTasks.add(task);
		}

		// warten, bis alle ausgeführt worden sind
		while (!runningTasks.isEmpty()) {
			if (!housekeepingReduce(runningTasks)) {
				// nur warten, wenn sich nichts getan hat
				Thread.sleep(200);
			}
		}
	}

	private boolean housekeepingReduce(List<ReduceWorkerTask> runningTasks) throws InterruptedException {
		List<ReduceWorkerTask> done = new LinkedList<ReduceWorkerTask>();
		List<ReduceWorkerTask> failed = new LinkedList<ReduceWorkerTask>();
		for (ReduceWorkerTask task : runningTasks) {
			switch (task.getCurrentState()) {
			case COMPLETED:
				done.add(task);
				break;
			case FAILED:
				failed.add(task);
				break;
			case ENQUEUED: // fall through
			case INITIATED: // fall through
			case INPROGRESS:
				// ok, wird demnaechst ausgefuehrt
				break;
			case ABORTED:
				throw new IllegalStateException("Task must not be in this list when in State ABORTED");
			default:
				throw new IllegalStateException("Unhandled state: " + task.getCurrentState());
			}
		}
		LOG.log(Level.FINE, "{0} Tasks done, {1} Tasks failed", new Object[] { done.size(), failed.size() });
		runningTasks.removeAll(done);
		runningTasks.addAll(restartFailedReduce(failed));
		return done.isEmpty() && !failed.isEmpty();
	}

	private List<ReduceWorkerTask> restartFailedReduce(List<ReduceWorkerTask> faileds) throws InterruptedException {
		List<ReduceWorkerTask> newtasks = new ArrayList<ReduceWorkerTask>(faileds.size());
		for (ReduceWorkerTask failed : faileds) {
			ReduceWorkerTask newtask = this.workerTaskFactory.createReduceWorkerTask(failed.getReduceInstruction(),
					failed.getInput(), failed.getValues(), failed.getPersistence());
			this.pool.enqueueTask(newtask);
			newtasks.add(newtask);
		}
		return newtasks;
	}
}
