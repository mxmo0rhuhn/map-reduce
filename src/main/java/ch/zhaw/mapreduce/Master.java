package ch.zhaw.mapreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

	public enum State {
		NONE, MAP, SHUFFLE, REDUCE
	}

	private State curState = State.NONE;

	private MapInstruction mapInstruction;
	private CombinerInstruction combinerInstruction;
	private ReduceInstruction reduceInstruction;

	// Prozentsatz der Aufgaben, die noch offen sein müssen bis rescheduled wird
	private static final int RESCHEDULESTARTPERCENTAGE = 10;
	// Alle n Warte-Durchläufe wird rescheduled
	private static final int RESCHEDULEEVERY = 10;
	// Wartezeit in millisekunden bis in einem Durchlauf wieder die Worker angefragt werden etc
	private static final int WAITTIME = 1000;

	@Inject
	private Logger logger;
	private final Provider<Shuffler> shufflerProvider;
	private final Pool pool;
	private final String mapReduceTaskUUID;
	private final WorkerTaskFactory runnerFactory;

	@Inject
	public Master(Pool pool, WorkerTaskFactory runnerFactory,
			@MapReduceTaskUUID String mapReduceTaskUUID, Provider<Shuffler> shufflerProvider) {
		this.pool = pool;
		this.runnerFactory = runnerFactory;
		this.mapReduceTaskUUID = mapReduceTaskUUID;
		this.shufflerProvider = shufflerProvider;
	}

	public Map<String, String> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction,
			final ReduceInstruction reduceInstruction, Iterator<String> input)
			throws InterruptedException {
		this.mapInstruction = mapInstruction;
		this.combinerInstruction = combinerInstruction;
		this.reduceInstruction = reduceInstruction;
		Set<KeyValuePair<String, WorkerTask>> activeTasks = new LinkedHashSet<KeyValuePair<String, WorkerTask>>();

		// MAP
		// Alle derzeitigen aufgaben die ausgeführt werden
		logger.info("MAP started");
		curState = State.MAP;
		Map<String, KeyValuePair> mapTasks = runMap(mapInstruction, combinerInstruction, input,
				activeTasks);
		logger.info("MAP all tasks enqueued");
		Set<WorkerTask> mapResults = waitForWorkers(activeTasks, mapTasks);
		logger.info("MAP done");

		// SHUFFLE
		logger.info("SHUFFLE started");
		curState = State.SHUFFLE;
		Shuffler s = createShuffler(mapResults);
		logger.info("SHUFFLE done");

		// REDUCE
		logger.info("REDUCE started");
		curState = State.REDUCE;
		Map<String, KeyValuePair> reduceInputs = runReduce(reduceInstruction, s.getResults(),
				activeTasks);
		logger.info("REDUCE all tasks enqueued");
		Set<WorkerTask> reduceResults = waitForWorkers(activeTasks, reduceInputs);
		logger.info("REDUCE done");

		// Collecting results
		logger.info("Collecting results started");
		curState = State.NONE;
		Map<String, String> results = collectResults(reduceResults);
		logger.info("Collecting results done");

		// Cleaning results from workers
		logger.info("Cleaning results started");
		this.pool.cleanResults(mapReduceTaskUUID);
		logger.info("Cleaning results done");
		return results;
	}

	Map<String, KeyValuePair> runMap(MapInstruction mapInstruction,
			CombinerInstruction combinerInstruction, Iterator<String> input,
			Set<KeyValuePair<String, WorkerTask>> activeTasks) {
		Map<String, KeyValuePair> inputToUuidMapping = new LinkedHashMap<String, KeyValuePair>();

		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (input.hasNext()) {

			String mapTaskUuid = UUID.randomUUID().toString();
			String todo = input.next();
			inputToUuidMapping
					.put(mapTaskUuid, new KeyValuePair<String, String>(mapTaskUuid, todo));

			MapWorkerTask mapTask = runnerFactory.createMapWorkerTask(mapReduceTaskUUID,
					mapTaskUuid, mapInstruction, combinerInstruction, todo);

			activeTasks.add(new KeyValuePair<String, WorkerTask>(mapTaskUuid, mapTask));
			pool.enqueueWork(mapTask);
		}
		return inputToUuidMapping;
	}

	Shuffler createShuffler(Collection<WorkerTask> mapResults) {
		Shuffler s = shufflerProvider.get();
		for (WorkerTask task : mapResults) {
			MapWorkerTask mapTask = (MapWorkerTask) task;
			for (KeyValuePair<String, String> curKeyValuePair : mapTask
					.getResults(mapReduceTaskUUID)) {
				s.put(curKeyValuePair.getKey(), curKeyValuePair.getValue());
			}
		}
		return s;
	}

	/**
	 * 
	 * @param reduceInstruction
	 * @param shuffleResults
	 * @param activeTasks
	 *            alle gestarteten reduce Tasks
	 * @return Alle Inputs
	 */
	Map<String, KeyValuePair> runReduce(ReduceInstruction reduceInstruction,
			Iterator<Map.Entry<String, List<KeyValuePair<String, String>>>> shuffleResults,
			Set<KeyValuePair<String, WorkerTask>> activeTasks) {

		Map<String, KeyValuePair> reduceToUuid = new LinkedHashMap<String, KeyValuePair>();

		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (shuffleResults.hasNext()) {
			Map.Entry<String, List<KeyValuePair<String, String>>> curKeyValuePairs = shuffleResults
					.next();
			KeyValuePair<String, List<KeyValuePair<String, String>>> curInput = new KeyValuePair<String, List<KeyValuePair<String, String>>>(
					curKeyValuePairs.getKey(), curKeyValuePairs.getValue());

			String reduceTaskUuid = UUID.randomUUID().toString();
			ReduceWorkerTask reduceTask = runnerFactory.createReduceWorkerTask(mapReduceTaskUUID,
					reduceTaskUuid, curInput.getKey(), reduceInstruction, curInput.getValue());

			activeTasks.add(new KeyValuePair<String, WorkerTask>(curInput.getKey(), reduceTask));
			pool.enqueueWork(reduceTask);
			reduceToUuid.put(curInput.getKey(), curInput);
		}
		return reduceToUuid;
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

	private Set<WorkerTask> waitForWorkers(Set<KeyValuePair<String, WorkerTask>> activeWorkerTasks,
			Map<String, KeyValuePair> input) throws InterruptedException {

		Map<String, KeyValuePair> inputTodo = input;
		Set<WorkerTask> results = new LinkedHashSet<WorkerTask>();
		Set<KeyValuePair> rescheduleInput = new LinkedHashSet<KeyValuePair>();
		Set<String> doneInput = new LinkedHashSet<String>();

		int rescheduleCounter = 0;
		// Fragt alle MapWorker Tasks an ob sie bereits erledigt sind - bis sie erledigt sind ...
		do {
			// Schauen welche Tasks noch ausstehend sind
			List<WorkerTask> toInactiveWorkerTasks = new LinkedList<WorkerTask>();
			for (KeyValuePair<String, WorkerTask> task : activeWorkerTasks) {
				switch (task.getValue().getCurrentState()) {
				case COMPLETED:
					logger.finer("Task completed");

					// nur übernehmen, wenn Aufgabe nicht bereits erledigt wurde
					if (!doneInput.contains(task.getKey())) {
						results.add(task.getValue());
						doneInput.add(task.getKey());
						inputTodo.remove(task.getKey());
					}
					toInactiveWorkerTasks.add(task.getValue());

					break;
				case FAILED:
					logger.finer("Task failed");
					toInactiveWorkerTasks.add(task.getValue());
					rescheduleInput.add(inputTodo.get(task.getKey()));

					break;
				case INPROGRESS:
					logger.finest("Task in progress");
					break;
				default:
					logger.fine("Task " + task.getValue().getCurrentState());
				}
			}
			activeWorkerTasks.removeAll(toInactiveWorkerTasks);

			// Ein gewisser Prozentsatz der Aufgaben ist erfüllt
			if ((doneInput.size() * 100) / input.size() <= RESCHEDULESTARTPERCENTAGE) {

				if (rescheduleCounter >= RESCHEDULEEVERY) {
					rescheduleInput.addAll(inputTodo.values());
					rescheduleCounter = 0;
				} else {
					rescheduleCounter++;
				}
			}

			reschedule(rescheduleInput, activeWorkerTasks);

			Thread.sleep(WAITTIME);
		} while (!activeWorkerTasks.isEmpty());
		return results;
	}

	private void reschedule(Set<KeyValuePair> rescheduleInput,
			Set<KeyValuePair<String, WorkerTask>> activeWorkerTasks) {
		switch (curState) {
		case MAP:
			for (KeyValuePair<String, String> rescheduleTodo : rescheduleInput) {

				MapWorkerTask mapTask = runnerFactory.createMapWorkerTask(mapReduceTaskUUID,
						rescheduleTodo.getKey(), mapInstruction, combinerInstruction,
						rescheduleTodo.getValue());

				activeWorkerTasks.add(new KeyValuePair<String, WorkerTask>(rescheduleTodo.getKey(),
						mapTask));
				pool.enqueueWork(mapTask);
			}
			break;
		case REDUCE:
			for (KeyValuePair<String, List<KeyValuePair<String, String>>> rescheduleTodo : rescheduleInput) {
				String reduceTaskUuid = UUID.randomUUID().toString();

				ReduceWorkerTask reduceTask = runnerFactory.createReduceWorkerTask(
						mapReduceTaskUUID, reduceTaskUuid, rescheduleTodo.getKey(),
						reduceInstruction, rescheduleTodo.getValue());

				activeWorkerTasks.add(new KeyValuePair<String, WorkerTask>(rescheduleTodo.getKey(),
						reduceTask));
				pool.enqueueWork(reduceTask);
			}

			break;
		default:
			break;
		}
	}
}