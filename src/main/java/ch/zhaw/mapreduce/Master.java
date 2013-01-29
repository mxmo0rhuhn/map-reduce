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

import javax.inject.Inject;

import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;
import ch.zhaw.mapreduce.registry.MapReduceTaskUUID;
import ch.zhaw.mapreduce.registry.Registry;

public final class Master {

	private final Pool pool;

	private final String mapReduceTaskUUID;

	private final WorkerTaskFactory runnerFactory;

	@Inject
	public Master(Pool pool, WorkerTaskFactory runnerFactory,
			@MapReduceTaskUUID String mapReduceTaskUUID) {
		this.pool = pool;
		this.runnerFactory = runnerFactory;
		this.mapReduceTaskUUID = mapReduceTaskUUID;
	}

	public Map<String, String> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction,
			final ReduceInstruction reduceInstruction, Iterator<String> input)
			throws InterruptedException {

		// MAP
		// Alle derzeitigen aufgaben die ausgeführt werden
		System.out.println("Starte MAP Phase");
		Set<WorkerTask> mapTasks = runMap(mapInstruction, combinerInstruction, input);
		Set<WorkerTask> mapResults = waitForWorkers(mapTasks);
		System.out.println("MAP Phase fertig");

		// SHUFFLE
		System.out.println("Starte SHUFFLE Phase");
		Shuffler s = createShuffler(mapResults);
		System.out.println("SHUFFLE Phase fertig");

		// REDUCE
		System.out.println("Starte REDUCE Phase");
		Set<WorkerTask> reduceTasks = runReduce(reduceInstruction, s.getResults());
		Set<WorkerTask> reduceResults = waitForWorkers(reduceTasks);
		System.out.println("REDUCE Phase fertig");

		Map<String, String> results = collectResults(reduceResults);

		this.pool.cleanResults(mapReduceTaskUUID);
		return results;
	}

	Set<WorkerTask> runMap(MapInstruction mapInstruction, CombinerInstruction combinerInstruction,
			Iterator<String> input) {
		Set<WorkerTask> activeWorkerTasks = new LinkedHashSet<WorkerTask>();
		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (input.hasNext()) {

			String inputUUID = UUID.randomUUID().toString();
			String todo = input.next();

			MapWorkerTask mapTask = runnerFactory.createMapWorkerTask(mapReduceTaskUUID,
					mapInstruction, combinerInstruction, inputUUID, todo);

			activeWorkerTasks.add(mapTask);
			pool.enqueueWork(mapTask);
		}
		return activeWorkerTasks;
	}

	Shuffler createShuffler(Collection<WorkerTask> mapResults) {
		Shuffler s = Registry.getComponent(Shuffler.class);
		for (WorkerTask curMapResult : mapResults) {
			for (KeyValuePair curKeyValuePair : curMapResult.getResults(mapReduceTaskUUID)) {
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

			ReduceWorkerTask reduceTask = runnerFactory.createReduceWorkerTask(mapReduceTaskUUID,
					curKeyValuePairs.getKey(), reduceInstruction, curKeyValuePairs.getValue());

			reduceTasks.add(reduceTask);
			pool.enqueueWork(reduceTask);
		}
		return reduceTasks;
	}

	Map<String, String> collectResults(Set<WorkerTask> reduceResults) {
		Map<String, String> resultStructure = new HashMap<String, String>();
		for (WorkerTask curWorker : reduceResults) {
			for (KeyValuePair storedValue : curWorker.getResults(mapReduceTaskUUID)) {
				resultStructure.put(storedValue.getKey(), storedValue.getValue());
			}
		}
		return resultStructure;
	}

	public String getMapReduceTaskUUID() {
		return this.mapReduceTaskUUID;
	}

	private Set<WorkerTask> waitForWorkers(Set<WorkerTask> activeWorkerTasks)
			throws InterruptedException {
		Set<WorkerTask> results = new HashSet<WorkerTask>();

		// Fragt alle MapWorker Tasks an ob sie bereits erledigt sind - bis sie erledigt sind ...
		do {
			// Schauen welche Tasks noch ausstehend sind
			List<WorkerTask> toRemove = new LinkedList<WorkerTask>();
			for (WorkerTask curWorkerTask : activeWorkerTasks) {
				switch (curWorkerTask.getCurrentState()) {
				case COMPLETED:
					toRemove.add(curWorkerTask);
					results.add(curWorkerTask);
					break;
				case FAILED:
					// System.out.println("State:  " + curWorkerTask.getCurrentState());
					// System.out.println("Worker: " + curWorkerTask.getWorker());
					break;
				default:

					// Falls es diesen Status überhaupt gibt

				}
			}
			activeWorkerTasks.removeAll(toRemove);
		} while (!activeWorkerTasks.isEmpty());
		return results;
	}
}
