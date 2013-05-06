package ch.zhaw.mapreduce.plugins.socket;

import java.net.InetAddress;
import java.net.UnknownHostException;

import ch.zhaw.mapreduce.WorkerTask;
import de.root1.simon.annotation.SimonRemote;

@SimonRemote(ClientCallback.class)
public class TestClientCallback implements ClientCallback {

	private static final long serialVersionUID = 5878055592823945127L;

	@Override
	public void helloslave() {
		System.out.println("Acknowledged");
	}

	@Override
	public Object runTask(WorkerTask task) {
		System.out.println("Run Task: " + task.getMapReduceTaskUUID() + " - " + task.getUUID());
		return null;
	}

	@Override
	public String getIp() {
		try {
			return InetAddress.getLocalHost().toString();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return "NOIP";
		}
	}

}