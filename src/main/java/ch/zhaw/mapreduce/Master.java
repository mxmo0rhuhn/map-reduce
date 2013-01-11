package ch.zhaw.mapreduce;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import ch.zhaw.mapreduce.registry.MapReduceTaskUUID;

public final class Master {

	private final String mapReduceTaskUUID;
	private final WorkerTaskFactory runnerFactory;

	@Inject
	public Master(WorkerTaskFactory runnerFactory, @MapReduceTaskUUID String mapReduceTaskUUID) {
		this.runnerFactory = runnerFactory;
		this.mapReduceTaskUUID = mapReduceTaskUUID;
	}

	public Map<String, String> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction,
			final ReduceInstruction reduceInstruction, Iterator<String> input) throws InterruptedException {

		// Alle derzeitigen aufgaben die ausgeführt werden
		Set<WorkerTask> activeWorkerTasks = new HashSet<WorkerTask>();
		// Eine Sammelung aus IDs von ausstehenden Tasks
		Set<String> undoneTasks = new HashSet<String>();

		// Die übersetzung welche Map ID welchen Input hat
		Map<String, String> mapTaskIDMapping = new HashMap<String, String>();

		// Alle Worker, die Ergebnisse besitzen
		Set<Worker> mapResults = new HashSet<Worker>();

		// MAP
		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (input.hasNext()) {

			String inputUUID = UUID.randomUUID().toString();
			String todo = input.next();

			MapWorkerTask mapTask = runnerFactory.createMapWorkerTask(mapReduceTaskUUID,
					mapInstruction, combinerInstruction, inputUUID, todo);

			activeWorkerTasks.add(mapTask);
			undoneTasks.add(inputUUID);
			mapTaskIDMapping.put(inputUUID, todo);
			mapTask.runMapTask();
		}

		waitForWorkers(mapResults, undoneTasks, activeWorkerTasks);

		Map<String, List<KeyValuePair>> reduceTasks = new HashMap<String, List<KeyValuePair>>();

		// SHUFFLE
		for (Worker curMapResult : mapResults) {
			for (KeyValuePair curKeyValuePair : curMapResult
					.getMapResults(mapReduceTaskUUID)) {
				if (reduceTasks.containsKey(curKeyValuePair.getKey())) {
					reduceTasks.get(curKeyValuePair.getKey()).add(curKeyValuePair);
				} else {
					List<KeyValuePair> newKeyValueList = new LinkedList<KeyValuePair>();
					newKeyValueList.add(curKeyValuePair);
					reduceTasks.put(curKeyValuePair.getKey(), newKeyValueList);
				}
			}
		}

		activeWorkerTasks = new HashSet<WorkerTask>();
		undoneTasks = new HashSet<String>();
		
		// Alle Worker, die Ergebnisse besitzen
		Set<Worker> reduceResults = new HashSet<Worker>();

		// REDUCE
		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		for (Map.Entry<String, List<KeyValuePair>> curKeyValuePairs : reduceTasks.entrySet()) {

			ReduceWorkerTask reduceTask = runnerFactory.createReduceWorkerTask(mapReduceTaskUUID,
					curKeyValuePairs.getKey(), reduceInstruction, curKeyValuePairs.getValue());

			activeWorkerTasks.add(reduceTask);
			undoneTasks.add(curKeyValuePairs.getKey());
			reduceTask.runReduceTask();
		}

		waitForWorkers(reduceResults, undoneTasks, activeWorkerTasks);
		
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

	private void waitForWorkers(Set<Worker> results, Set<String> undoneTasks,
			Set<WorkerTask> activeWorkerTasks) throws InterruptedException {
		Set<String> doneTasks = new HashSet<String>(undoneTasks.size());

		// Fragt alle MapWorker Tasks an ob sie bereits erledigt sind - bis sie erledigt sind ...
		do {
			// Schauen welche Tasks noch ausstehend sind
			for (WorkerTask curWorkerTask : activeWorkerTasks) {
				switch (curWorkerTask.getCurrentState()) {
				case COMPLETED:
					// Aufgabe war noch nicht erledigt
					if (!doneTasks.contains(curWorkerTask.getUUID())) {
						doneTasks.add(curWorkerTask.getUUID());
						results.add(curWorkerTask.getWorker());
						undoneTasks.remove(curWorkerTask.getUUID());
					}
				case FAILED:

					// Falls es diesen Status überhaupt gibt

				}
			}
			// Wartet eine Sekunde abzüglich der Prozentzahl an bereits erledigten Aufgaben
			// "+1" um die DIV/0 zu verhindern
			Thread.sleep(1000 - 1000 * (doneTasks.size() / (doneTasks.size() + undoneTasks.size() + 1)));

		} while (undoneTasks.size() > 0);
	}
}
