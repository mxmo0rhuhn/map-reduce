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

import ch.zhaw.mapreduce.registry.MapReduceTaskUUID;

public final class Master {
	
	private final Pool pool;

	private final String mapReduceTaskUUID;
	
	private final WorkerTaskFactory runnerFactory;

	@Inject
	public Master(Pool pool, WorkerTaskFactory runnerFactory, @MapReduceTaskUUID String mapReduceTaskUUID) {
		this.pool = pool;
		this.runnerFactory = runnerFactory;
		this.mapReduceTaskUUID = mapReduceTaskUUID;
	}

	public Map<String, String> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction,
			final ReduceInstruction reduceInstruction, Iterator<String> input) throws InterruptedException {

		// MAP
		// Alle derzeitigen aufgaben die ausgeführt werden
		Set<WorkerTask> mapTasks = runMap(mapInstruction, combinerInstruction, input);
		Set<Worker> mapResults = waitForWorkers(mapTasks);

		// SHUFFLE
		Map<String, List<KeyValuePair>> shuffleResults = runShuffle(mapResults);

		// REDUCE
		Set<WorkerTask> reduceTasks = runReduce(reduceInstruction, shuffleResults);
		Set<Worker> reduceResults = waitForWorkers(reduceTasks);
		
		Map<String, String> results = collectResults(reduceResults);

		this.pool.cleanResults(mapReduceTaskUUID);
		return results;
	}
	
	Set<WorkerTask> runMap(MapInstruction mapInstruction, CombinerInstruction combinerInstruction, Iterator<String> input) {
		Set<WorkerTask> activeWorkerTasks = new LinkedHashSet<WorkerTask>();
		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (input.hasNext()) {

			String inputUUID = UUID.randomUUID().toString();
			String todo = input.next();

			MapWorkerTask mapTask = runnerFactory.createMapWorkerTask(mapReduceTaskUUID,
					mapInstruction, combinerInstruction, inputUUID, todo);

			activeWorkerTasks.add(mapTask);
			mapTask.runMapTask();
		}
		return activeWorkerTasks;
	}
	
	Map<String, List<KeyValuePair>> runShuffle (Collection<Worker> mapResults) {
		Map<String, List<KeyValuePair>> reduceTasks = new HashMap<String, List<KeyValuePair>>();
		for (Worker curMapResult : mapResults) {
			for (KeyValuePair curKeyValuePair : curMapResult.getMapResults(mapReduceTaskUUID)) {
				if (reduceTasks.containsKey(curKeyValuePair.getKey())) {
					reduceTasks.get(curKeyValuePair.getKey()).add(curKeyValuePair);
				} else {
					List<KeyValuePair> newKeyValueList = new LinkedList<KeyValuePair>();
					newKeyValueList.add(curKeyValuePair);
					reduceTasks.put(curKeyValuePair.getKey(), newKeyValueList);
				}
			}
		}
		return reduceTasks;
	}
	
	Set<WorkerTask> runReduce(ReduceInstruction reduceInstruction, Map<String, List<KeyValuePair>> shuffleResults) {
		Set<WorkerTask> reduceTasks = new LinkedHashSet<WorkerTask>();
		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		for (Map.Entry<String, List<KeyValuePair>> curKeyValuePairs : shuffleResults.entrySet()) {

			ReduceWorkerTask reduceTask = runnerFactory.createReduceWorkerTask(mapReduceTaskUUID,
					curKeyValuePairs.getKey(), reduceInstruction, curKeyValuePairs.getValue());

			reduceTasks.add(reduceTask);
			reduceTask.runReduceTask();
		}
		return reduceTasks;
	}
	
	Map<String, String> collectResults (Set<Worker> reduceResults) {
		Map<String, String> resultStructure = new HashMap<String, String>();
		for(Worker curWorker : reduceResults) {
			for(KeyValuePair storedValue : curWorker.getReduceResults(mapReduceTaskUUID)) {
				resultStructure.put(storedValue.getKey(), storedValue.getValue());
			}
		}
		return resultStructure;
	}

	public String getMapReduceTaskUUID() {
		return this.mapReduceTaskUUID;
	}

	private Set<Worker> waitForWorkers(Set<WorkerTask> activeWorkerTasks) throws InterruptedException {
		Set<Worker> workerWithResults = new HashSet<Worker>();

		// Fragt alle MapWorker Tasks an ob sie bereits erledigt sind - bis sie erledigt sind ...
		do {
			// Schauen welche Tasks noch ausstehend sind
			List<WorkerTask> toRemove = new LinkedList<WorkerTask>();
			for (WorkerTask curWorkerTask : activeWorkerTasks) {
				switch (curWorkerTask.getCurrentState()) {
				case COMPLETED:
					toRemove.add(curWorkerTask);
					workerWithResults.add(curWorkerTask.getWorker());
				case FAILED:

					// Falls es diesen Status überhaupt gibt

				}
			}
			activeWorkerTasks.removeAll(toRemove);
		} while (!activeWorkerTasks.isEmpty());
		return workerWithResults;
	}
}
