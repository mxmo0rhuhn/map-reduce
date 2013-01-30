package ch.zhaw.mapreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;
import ch.zhaw.mapreduce.registry.MapReduceTaskUUID;

import com.google.inject.Provider;

public final class Master {

	@Inject
	private Logger logger;

	private final Provider<Shuffler> shufflerProvider;

	private final Pool pool;

	private final String mapReduceTaskUUID;

	private final WorkerTaskFactory runnerFactory;

	@Inject
	public Master(Pool pool, WorkerTaskFactory runnerFactory, @MapReduceTaskUUID String mapReduceTaskUUID,
			Provider<Shuffler> shufflerProvider) {
		this.pool = pool;
		this.runnerFactory = runnerFactory;
		this.mapReduceTaskUUID = mapReduceTaskUUID;
		this.shufflerProvider = shufflerProvider;
	}

	public Map<String, String> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction, final ReduceInstruction reduceInstruction,
			Iterator<String> input) throws InterruptedException {
		// MAP
		// Alle derzeitigen aufgaben die ausgeführt werden
		logger.info("MAP started");
		Set<WorkerTask> mapTasks = runMap(mapInstruction, combinerInstruction, input);
		logger.info("MAP all tasks enqueued");
		Set<WorkerTask> mapResults = waitForWorkers(mapTasks);
		logger.info("MAP done");

		// SHUFFLE
		logger.info("SHUFFLE started");
		Shuffler s = createShuffler(mapResults);
		logger.info("SHUFFLE done");

		// REDUCE
		logger.info("REDUCE started");
		Set<WorkerTask> reduceTasks = runReduce(reduceInstruction, s.getResults());
		logger.info("REDUCE all tasks enqueued");
		Set<WorkerTask> reduceResults = waitForWorkers(reduceTasks);
		logger.info("REDUCE done");

		// Collecting results
		logger.info("Collecting results started");
		Map<String, String> results = collectResults(reduceResults);
		logger.info("Collecting results done");

		// Cleaning results from workers
		logger.info("Cleaning results started");
		this.pool.cleanResults(mapReduceTaskUUID);
		logger.info("Cleaning results done");
		return results;
	}

	Set<WorkerTask> runMap(MapInstruction mapInstruction, CombinerInstruction combinerInstruction,
			Iterator<String> input) {
		Set<WorkerTask> activeWorkerTasks = new LinkedHashSet<WorkerTask>();
		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (input.hasNext()) {

			String mapTaskUuid = UUID.randomUUID().toString();
			String todo = input.next();

			MapWorkerTask mapTask = runnerFactory.createMapWorkerTask(mapReduceTaskUUID, mapTaskUuid, mapInstruction,
					combinerInstruction, todo);

			activeWorkerTasks.add(mapTask);
			pool.enqueueWork(mapTask);
		}
		return activeWorkerTasks;
	}

	Shuffler createShuffler(Collection<WorkerTask> mapResults) {
		Shuffler s = shufflerProvider.get();
		for (WorkerTask task : mapResults) {
			MapWorkerTask mapTask = (MapWorkerTask) task;
			for (KeyValuePair curKeyValuePair : mapTask.getResults(mapReduceTaskUUID)) {
				s.put(curKeyValuePair.getKey(), curKeyValuePair.getValue());
			}
		}
		return s;
	}

	Set<WorkerTask> runReduce(ReduceInstruction reduceInstruction,
			Iterator<Map.Entry<String, List<KeyValuePair>>> shuffleResults) {
		Set<WorkerTask> reduceTasks = new LinkedHashSet<WorkerTask>();
		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (shuffleResults.hasNext()) {
			Map.Entry<String, List<KeyValuePair>> curKeyValuePairs = shuffleResults.next();

			String reduceTaskUuid = UUID.randomUUID().toString();
			ReduceWorkerTask reduceTask = runnerFactory.createReduceWorkerTask(mapReduceTaskUUID, reduceTaskUuid,
					curKeyValuePairs.getKey(), reduceInstruction, curKeyValuePairs.getValue());

			reduceTasks.add(reduceTask);
			pool.enqueueWork(reduceTask);
		}
		return reduceTasks;
	}

	Map<String, String> collectResults(Set<WorkerTask> reduceResults) {
		Map<String, String> resultStructure = new HashMap<String, String>();
		for (WorkerTask task : reduceResults) {
			ReduceWorkerTask reduceTask = (ReduceWorkerTask) task;
			for (String value : reduceTask.getResults(mapReduceTaskUUID)) {
				resultStructure.put(reduceTask.getInput(), value);
			}
		}
		return resultStructure;
	}

	public String getMapReduceTaskUUID() {
		return this.mapReduceTaskUUID;
	}

	private Set<WorkerTask> waitForWorkers(Set<WorkerTask> activeWorkerTasks) throws InterruptedException {
		Set<WorkerTask> results = new HashSet<WorkerTask>();

		// Fragt alle MapWorker Tasks an ob sie bereits erledigt sind - bis sie erledigt sind ...
		do {
			// Schauen welche Tasks noch ausstehend sind
			List<WorkerTask> toRemove = new LinkedList<WorkerTask>();
			for (WorkerTask task : activeWorkerTasks) {
				switch (task.getCurrentState()) {
				case COMPLETED:
					logger.finer("Task completed");
					toRemove.add(task);
					results.add(task);
					break;
				case FAILED:
					logger.finer("Task failed");
					break;
				case INPROGRESS:
					logger.finest("Task in progress");
					break;
				default:
					logger.fine("Task " + task.getCurrentState());
				}
			}
			activeWorkerTasks.removeAll(toRemove);
		} while (!activeWorkerTasks.isEmpty());
		return results;
	}
}
