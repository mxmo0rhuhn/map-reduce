package ch.zhaw.mapreduce.plugins.socket;

import java.util.Arrays;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapReduceUtil;
import ch.zhaw.mapreduce.WorkerTask;
import de.root1.simon.annotation.SimonRemote;

@SimonRemote(SocketAgent.class)
public class TestSocketAgent implements SocketAgent {

	@Override
	public void helloslave() {
		System.out.println("Acknowledged");
	}

	@Override
	public Object runTask(WorkerTask task) {
		System.out.println("Run Task: " + task.getMapReduceTaskUuid() + " - " + task.getTaskUuid());
		return Arrays.asList(new KeyValuePair[]{new KeyValuePair("key1", "val1")});
	}

	@Override
	public String getIp() {
		return MapReduceUtil.getLocalIp();
	}

}