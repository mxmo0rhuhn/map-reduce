package ch.zhaw.mapreduce;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

public final class Master {

	private static final Logger LOG = Logger.getLogger(Master.class.getName());

	private final Shuffler shuffler;

	private final Provider<Persistence> persistenceProvider;

	private final ScheduledExecutorService supervisorService;
	private final Pool pool;
	private final WorkerTaskFactory workerTaskFactory;
	private final ExecutorService executorPool = Executors.newCachedThreadPool();
	private final long statisticsPrintTimeout;
	private volatile int currentTaskPercentage;

	@Inject
	Master(Pool pool, WorkerTaskFactory workerTaskFactory, Shuffler shuffler,
			Provider<Persistence> persistenceProvider,
			@Named("supervisorScheduler") ScheduledExecutorService supervisorService,
			@Named("statisticsPrinterTimeout") long statisticsTimeout) {
		this.pool = pool;
		this.workerTaskFactory = workerTaskFactory;
		this.shuffler = shuffler;
		this.persistenceProvider = persistenceProvider;
		this.supervisorService = supervisorService;
		this.statisticsPrintTimeout = statisticsTimeout;
	}

	@PostConstruct
	public void startSupervisor() {
		this.supervisorService.scheduleAtFixedRate((new Runnable() {
			@Override
			public void run() {
				LOG.info("Master: " + currentTaskPercentage + " % Tasks processed");
			}
		}), statisticsPrintTimeout, statisticsPrintTimeout, TimeUnit.MILLISECONDS);
	}

	public Map<String, List<String>> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction, final ReduceInstruction reduceInstruction,
			ShuffleProcessorFactory afterShuffleHook, Iterator<String> inputs) throws InterruptedException {

		// für sämtliche tasks einer berechnung muss die gleiche persistence verwendet werden!
		Persistence pers = this.persistenceProvider.get();

		LOG.info("Start Triggering Map Tasks");
		List<MapWorkerTask> mapTasks = triggerMapTasks(mapInstruction, combinerInstruction, inputs, pers);
		LOG.info("Done Triggering Map Tasks");

		LOG.info("Start Joining and Rescheduling Map Tasks");
		joinAndRescheduleMap(mapTasks);
		LOG.info("Done Joining and Rescheduling Map Tasks");

		LOG.info("Retrieve all Map Results from Persistence");
		List<KeyValuePair> mapResults = pers.getMapResults();

		LOG.info("Shuffle all Map Results");
		Map<String, List<KeyValuePair>> shuffledResults = shuffler.shuffle(mapResults);
		LOG.info("Done shuffling Map Results");

		if (afterShuffleHook != null) {
			LOG.info("Running AfterShuffleHook with Exector");
			executorPool.execute(afterShuffleHook.getNewRunnable(shuffledResults.entrySet().iterator()));
		} else {
			LOG.info("No AfterShuffleHook");
		}

		LOG.info("Start Triggering Reduce Tasks");
		List<ReduceWorkerTask> reduceTasks = triggerReduceTasks(reduceInstruction, shuffledResults, pers);
		LOG.info("Done Triggering Reduce Tasks");

		LOG.info("Start Joining and Rescheduling Reduce Tasks");
		joinAndRescheduleReduce(reduceTasks);
		LOG.info("Done Joining and Rescheduling Reduce Tasks");

		LOG.info("Retrieve all Reduce Results from Persistence");
		Map<String, List<String>> results = pers.getReduceResults();

		LOG.info("Kill Persistence");
		pers.suicide();

		return results;
	}

	private void joinAndRescheduleReduce(List<ReduceWorkerTask> tasks) throws InterruptedException {
		if (tasks.isEmpty()) {
			LOG.fine("Nothing to join or reschedule");
			return;
		}
		Thread.sleep(200);
		List<ReduceWorkerTask> done = new LinkedList<ReduceWorkerTask>();
		List<ReduceWorkerTask> failed = new LinkedList<ReduceWorkerTask>();
		for (ReduceWorkerTask task : tasks) {
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
		tasks.removeAll(done);
		tasks.addAll(restartFailedReduce(failed));
		joinAndRescheduleReduce(tasks);
	}

	private List<ReduceWorkerTask> triggerReduceTasks(ReduceInstruction reduceInstruction,
			Map<String, List<KeyValuePair>> shuffledResults, Persistence pers) throws InterruptedException {
		List<ReduceWorkerTask> reduceTasks = new LinkedList<ReduceWorkerTask>();
		for (Entry<String, List<KeyValuePair>> entry : shuffledResults.entrySet()) {
			String key = entry.getKey();
			List<KeyValuePair> values = entry.getValue();
			ReduceWorkerTask task = this.workerTaskFactory.createReduceWorkerTask(reduceInstruction, key, values, pers);
			this.pool.enqueueTask(task);
			reduceTasks.add(task);
		}
		return reduceTasks;
	}

	private List<MapWorkerTask> triggerMapTasks(MapInstruction mapInstruction, CombinerInstruction combinerInstruction,
			Iterator<String> inputs, Persistence pers) throws InterruptedException {

		List<MapWorkerTask> mapTasks = new LinkedList<MapWorkerTask>();
		while (inputs.hasNext()) {
			String input = inputs.next();
			MapWorkerTask task = this.workerTaskFactory.createMapWorkerTask(mapInstruction, combinerInstruction, input,
					pers);
			this.pool.enqueueTask(task);
			mapTasks.add(task);
		}
		return mapTasks;
	}

	private void joinAndRescheduleMap(List<MapWorkerTask> tasks) throws InterruptedException {
		if (tasks.isEmpty()) {
			LOG.fine("Nothing to join or reschedule");
			return;
		}
		Thread.sleep(200);
		List<MapWorkerTask> done = new LinkedList<MapWorkerTask>();
		List<MapWorkerTask> failed = new LinkedList<MapWorkerTask>();
		for (MapWorkerTask task : tasks) {
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
		tasks.removeAll(done);
		tasks.addAll(restartFailedMap(failed));
		joinAndRescheduleMap(tasks);
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

	private List<MapWorkerTask> restartFailedMap(List<MapWorkerTask> faileds) throws InterruptedException {
		List<MapWorkerTask> newtasks = new ArrayList<MapWorkerTask>(faileds.size());
		for (MapWorkerTask failed : faileds) {
			MapWorkerTask newtask = this.workerTaskFactory.createMapWorkerTask(failed.getMapInstruction(),
					failed.getCombinerInstruction(), failed.getInput(), failed.getPersistence());
			this.pool.enqueueTask(newtask);
			newtasks.add(newtask);
		}
		return newtasks;
	}
}
