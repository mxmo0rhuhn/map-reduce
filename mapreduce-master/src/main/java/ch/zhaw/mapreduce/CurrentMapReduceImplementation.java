/**
 * 
 */
package ch.zhaw.mapreduce;

import java.util.Iterator;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.assistedinject.Assisted;

/**
 * Stellt die Serverseitige Implementation des MapReduce Frameworks dar.
 * 
 * @author Max
 * 
 */
public class CurrentMapReduceImplementation implements MapReduce {

	private MapInstruction mapInstruction;
	private ReduceInstruction reduceInstruction;
	private CombinerInstruction combinerInstruction;

	private Master master;
	private static Injector currentMRConfig;
	private static ServerStarter server;

	/*
	 * (non-Javadoc)
	 * 
	 * @see ch.zhaw.mapreduce.MapReduce#start()
	 */
	@Override
	public void start() {
		currentMRConfig = Guice.createInjector(new MapReduceConfig());
		server = new ServerStarter(currentMRConfig);
		server.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ch.zhaw.mapreduce.MapReduce#stop()
	 */
	@Override
	public void stop() {
		server.stop();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ch.zhaw.mapreduce.MapReduce#newMRTask(ch.zhaw.mapreduce.MapInstruction,
	 * ch.zhaw.mapreduce.ReduceInstruction, ch.zhaw.mapreduce.CombinerInstruction, java.util.Map)
	 */
	@Override
	public MapReduce newMRTask(MapInstruction mapInstruction, ReduceInstruction reduceInstruction,
			CombinerInstruction combinerInstruction, Map<String, String> config) {

		this.mapInstruction = mapInstruction;
		this.reduceInstruction = reduceInstruction;
		this.combinerInstruction = combinerInstruction;

		int rescheduleStartPercentage = 90;
		int rescheduleEvery = 10;
		int waitTime = 1000;
		if (config.containsKey("rescheduleStartPercentage")) {
			rescheduleStartPercentage = Integer.parseInt(config.get("rescheduleStartPercentage"));
		}

		if (config.containsKey("rescheduleEvery")) {
			rescheduleEvery = Integer.parseInt(config.get("rescheduleEvery"));
		}

		if (config.containsKey("waitTime")) {
			waitTime = Integer.parseInt(config.get("waitTime"));
		}

		this.master = currentMRConfig.getInstance(MasterFactory.class).createMaster(rescheduleStartPercentage, rescheduleEvery, waitTime);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see ch.zhaw.mapreduce.MapReduce#runMapReduceTask(java.util.Iterator)
	 */
	@Override
	public Map<String, String> runMapReduceTask(Iterator<String> input) {
		try {
			return this.master.runComputation(this.mapInstruction, this.combinerInstruction,
					this.reduceInstruction, input);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("Computation aborted.");
		}
	}

}
