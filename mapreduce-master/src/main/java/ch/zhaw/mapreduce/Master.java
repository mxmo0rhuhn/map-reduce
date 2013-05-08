package ch.zhaw.mapreduce;

import java.util.Collection;
import java.util.HashMap;
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
import javax.inject.Named;

import ch.zhaw.mapreduce.impl.MapWorkerTask;
import ch.zhaw.mapreduce.impl.ReduceWorkerTask;

import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;

public final class Master {

	public enum State {
		NONE, MAP, SHUFFLE, REDUCE
	}

	private State curState = State.NONE;

	private MapInstruction mapInstruction;
	private CombinerInstruction combinerInstruction;
	private ReduceInstruction reduceInstruction;

	// Prozentsatz der Aufgaben, die noch offen sein müssen bis rescheduled wird
	private final int rescheduleStartPercentage;
	// Alle n Warte-Durchläufe wird rescheduled
	private final int rescheduleEvery;
	// Wartezeit in millisekunden bis in einem Durchlauf wieder die Worker angefragt werden etc
	private final int waitTime;

	private Logger logger = Logger.getLogger(Master.class.getName());

	private final Provider<Shuffler> shufflerProvider;

	private final Pool pool;
	private final String mapReduceTaskUuid;
	private final WorkerTaskFactory workerTaskFactory;

	@Inject
	Master(Pool pool, WorkerTaskFactory workerTaskFactory,
			@Named("mapReduceTaskUuid") String mapReduceTaskUuid,
			Provider<Shuffler> shufflerProvider,
			@Assisted("rescheduleStartPercentage") int rescheduleStartPercentage,
			@Assisted("rescheduleEvery") int rescheduleEvery, @Assisted("waitTime") int waitTime) {
		this.pool = pool;
		this.workerTaskFactory = workerTaskFactory;
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.shufflerProvider = shufflerProvider;

		this.rescheduleStartPercentage = rescheduleStartPercentage;
		this.rescheduleEvery = rescheduleEvery;
		this.waitTime = waitTime;
	}

	public Map<String, String> runComputation(final MapInstruction mapInstruction,
			final CombinerInstruction combinerInstruction,
			final ReduceInstruction reduceInstruction, Iterator<String> input)
			throws InterruptedException {
		this.mapInstruction = mapInstruction;
		this.combinerInstruction = combinerInstruction;
		this.reduceInstruction = reduceInstruction;

		// Alle gerade in der ausführung befindlichen Worker Tasks
		Set<KeyValuePair<String, WorkerTask>> activeTasks = new LinkedHashSet<KeyValuePair<String, WorkerTask>>();

		// MAP
		// Alle derzeitigen aufgaben die ausgeführt werden
		logger.info("MAP started");
		curState = State.MAP;
		Map<String, KeyValuePair> mapTasks = runMap(mapInstruction, combinerInstruction, input,
				activeTasks);
		logger.info("MAP " + mapTasks.size() + " tasks enqueued");
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
		logger.info("REDUCE " + reduceInputs.size() + " tasks enqueued");
		Set<WorkerTask> reduceResults = waitForWorkers(activeTasks, reduceInputs);
		logger.info("REDUCE done");

		// Collecting results
		logger.info("Collecting results started");
		curState = State.NONE;
		Map<String, String> results = collectResults(reduceResults);
		logger.info("Collected " + results.size() + " results");

		// Cleaning results from workers
		logger.info("Cleaning results started");
		this.pool.cleanResults(mapReduceTaskUuid);
		logger.info("Cleaning results done");
		return results;
	}

	/**
	 * Erstellt für jeden Teil des Inputs eine UUID und ein Mapping auf den input und führt danach
	 * jeweils einen WorkerTask mit diesem input aus
	 * 
	 * @param mapInstruction
	 *            die Map Anweisung die berechnet werden soll
	 * @param combinerInstruction
	 *            ggf die Combine Instruction, die vor der Rückgabe des Ergebnisses ausgeführt
	 *            werden soll
	 * @param input
	 *            iterator über alle Input Teile
	 * @param activeTasks
	 *            eine Liste in der alle derzeit aktiven Tasks abgelegt sind
	 * @return Ein Mapping von UUID auf ein KeyValue Pair UUID und zugehöriger Input
	 */
	Map<String, KeyValuePair> runMap(MapInstruction mapInstruction,
			CombinerInstruction combinerInstruction, Iterator<String> input,
			Set<KeyValuePair<String, WorkerTask>> activeTasks) {

		Map<String, KeyValuePair> uuidToInputMapping = new LinkedHashMap<String, KeyValuePair>();

		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (input.hasNext()) {

			String mapTaskUuid = UUID.randomUUID().toString();
			String todo = input.next();
			uuidToInputMapping
					.put(mapTaskUuid, new KeyValuePair<String, String>(mapTaskUuid, todo));

			MapWorkerTask mapTask = workerTaskFactory.createMapWorkerTask(mapReduceTaskUuid,
					mapInstruction, combinerInstruction, todo);

			activeTasks.add(new KeyValuePair<String, WorkerTask>(mapTaskUuid, mapTask));
			pool.enqueueWork(mapTask);
		}
		return uuidToInputMapping;
	}

	Shuffler createShuffler(Collection<WorkerTask> mapResults) {
		Shuffler s = shufflerProvider.get();
		for (WorkerTask task : mapResults) {
			MapWorkerTask mapTask = (MapWorkerTask) task;
			for (KeyValuePair<String, String> curKeyValuePair : mapTask.getResults()) {
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
			Iterator<Map.Entry<String, List<KeyValuePair>>> shuffleResults,
			Set<KeyValuePair<String, WorkerTask>> activeTasks) {

		Map<String, KeyValuePair> reduceToUuid = new LinkedHashMap<String, KeyValuePair>();

		// reiht für jeden Input - Teil einen MapWorkerTask in den Pool ein
		while (shuffleResults.hasNext()) {
			Map.Entry<String, List<KeyValuePair>> curKeyValuePairs = shuffleResults.next();
			KeyValuePair<String, List<KeyValuePair>> curInput = new KeyValuePair<String, List<KeyValuePair>>(
					curKeyValuePairs.getKey(), curKeyValuePairs.getValue());

			String reduceTaskUuid = UUID.randomUUID().toString();
			ReduceWorkerTask reduceTask = workerTaskFactory.createReduceWorkerTask(
					mapReduceTaskUuid, curInput.getKey(), reduceInstruction, curInput.getValue());

			activeTasks.add(new KeyValuePair<String, WorkerTask>(curInput.getKey(), reduceTask));
			pool.enqueueWork(reduceTask);
			reduceToUuid.put(curInput.getKey(), curInput);
		}
		return reduceToUuid;
	}

	/**
	 * Sammelt alle results aus den fertiggestellten Reduce WorkerTasks
	 * 
	 * @param reduceResults
	 *            ein Set mit fertiggestellten Reduce Tasks
	 * @return
	 */
	Map<String, String> collectResults(Set<WorkerTask> reduceResults) {
		Map<String, String> resultStructure = new HashMap<String, String>();
		for (WorkerTask task : reduceResults) {
			ReduceWorkerTask reduceTask = (ReduceWorkerTask) task;
			for (String value : reduceTask.getResults()) {
				resultStructure.put(reduceTask.getInput(), value);
			}
		}
		return resultStructure;
	}

	/**
	 * Gibt die UUID der MapReduce Aufgabe zurück
	 * 
	 * @return die UUID
	 */
	public String getMapReduceTaskUuid() {
		return this.mapReduceTaskUuid;
	}

	/**
	 * Wartet auf die gegebenen Worker und führt ab einem gewissen Schwellwert die verbleibenden
	 * Inputwerte redundant aus.
	 * 
	 * @param activeWorkerTasks
	 *            die derzeit aktiven WorkerTasks
	 * @param uuidToKeyValuePairUUIDInputMapping
	 *            ein Mapping von InputUUID auf KeyValuePairs aus &lt;InputUUID, Input&gt;
	 * @return gibt ein Set mit wirklich ausgeführten Workern zurück
	 * @throws InterruptedException
	 */
	private Set<WorkerTask> waitForWorkers(Set<KeyValuePair<String, WorkerTask>> activeWorkerTasks,
			Map<String, KeyValuePair> originalUuidToKeyValuePairUUIDInputMapping)
			throws InterruptedException {

		Map<String, KeyValuePair> remainingUuidMapping = new HashMap<String, KeyValuePair>(
				originalUuidToKeyValuePairUUIDInputMapping);
		Set<WorkerTask> results = new LinkedHashSet<WorkerTask>();
		Set<KeyValuePair> rescheduleInput = new LinkedHashSet<KeyValuePair>();
		Set<String> doneInputUUIDs = new LinkedHashSet<String>();

		int rescheduleCounter = 0;
		List<WorkerTask> toInactiveWorkerTasks = new LinkedList<WorkerTask>();

		// Fragt alle Tasks an ob sie bereits erledigt sind - bis sie erledigt sind ...
		do {
			Thread.sleep(waitTime);
			// Schauen welche Tasks noch ausstehend sind
			for (KeyValuePair<String, WorkerTask> task : activeWorkerTasks) {
				switch (task.getValue().getCurrentState()) {
				case COMPLETED:
					logger.finer("Task completed");

					// nur übernehmen, wenn Aufgabe nicht bereits erledigt wurde
					if (!doneInputUUIDs.contains(task.getKey())) {
						results.add(task.getValue());
						doneInputUUIDs.add(task.getKey());
						remainingUuidMapping.remove(task.getKey());
					}
					toInactiveWorkerTasks.add(task.getValue());

					break;
				case FAILED:
				case ABORTED:
					logger.finer("Task failed");
					toInactiveWorkerTasks.add(task.getValue());
					rescheduleInput.add(remainingUuidMapping.get(task.getKey()));

					break;
				case INPROGRESS:
				case ENQUEUED:
				case INITIATED:
					logger.finest("Task in progress");
					break;
				default:
					throw new IllegalStateException(task.getValue().getCurrentState().toString());
				}
			}
			activeWorkerTasks.removeAll(toInactiveWorkerTasks);

			// Ein gewisser Prozentsatz der Aufgaben ist erfüllt
			if ((doneInputUUIDs.size() * 100) / originalUuidToKeyValuePairUUIDInputMapping.size() >= rescheduleStartPercentage) {

				if (rescheduleCounter >= rescheduleEvery) {
					rescheduleInput.addAll(remainingUuidMapping.values());
					logger.info("Reschedule workers has started for " + remainingUuidMapping.size()
							+ " Workers");
					rescheduleCounter = 0;
				} else {
					rescheduleCounter++;
				}
			}

			// TODO Max: logging hier mit sinnvollen angaben (z.B. Anzahl reschedulbarer Tasks)
			reschedule(rescheduleInput, activeWorkerTasks);

		} while (!remainingUuidMapping.isEmpty());
		stopAndCleanTasks(activeWorkerTasks);
		return results;
	}

	/**
	 * Startet, abhängig von der derzeitigen Phase des Masters Map oder reduce Tasks
	 * 
	 * @param rescheduleInput
	 *            der Input für die Map oder Reduce Tasks
	 * @param activeWorkerTasks
	 *            eine Liste mit allen derzeit aktiven WorkerTasks
	 */
	private void reschedule(Set<KeyValuePair> rescheduleInput,
			Set<KeyValuePair<String, WorkerTask>> activeWorkerTasks) {
		switch (curState) {
		case MAP:
			for (KeyValuePair<String, String> rescheduleTodo : rescheduleInput) {

				MapWorkerTask mapTask = workerTaskFactory.createMapWorkerTask(mapReduceTaskUuid,
						mapInstruction, combinerInstruction, rescheduleTodo.getValue());

				activeWorkerTasks.add(new KeyValuePair<String, WorkerTask>(rescheduleTodo.getKey(),
						mapTask));
				pool.enqueueWork(mapTask);
			}
			break;
		case REDUCE:
			for (KeyValuePair<String, List<KeyValuePair>> rescheduleTodo : rescheduleInput) {
				String reduceTaskUuid = UUID.randomUUID().toString();

				ReduceWorkerTask reduceTask = workerTaskFactory.createReduceWorkerTask(
						mapReduceTaskUuid, rescheduleTodo.getKey(), reduceInstruction,
						rescheduleTodo.getValue());

				activeWorkerTasks.add(new KeyValuePair<String, WorkerTask>(rescheduleTodo.getKey(),
						reduceTask));
				pool.enqueueWork(reduceTask);
			}

			break;
		default:
			throw new IllegalStateException("Not in a Map or Reduce phase");
		}
	}

	private void stopAndCleanTasks(Set<KeyValuePair<String, WorkerTask>> activeWorkerTasks) {
		for (KeyValuePair<String, WorkerTask> curKV : activeWorkerTasks) {
			curKV.getValue().abort();
		}
		activeWorkerTasks.clear();
	}

	public int getRescheduleStartPercentage() {
		return rescheduleStartPercentage;
	}

	public int getRescheduleEvery() {
		return rescheduleEvery;
	}

	public int getWaitTime() {
		return waitTime;
	}
}