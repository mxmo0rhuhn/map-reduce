package ch.zhaw.mapreduce;

import java.util.List;


public class SocketAgent {
	
	void waitForMessage() {
		// warten bis was (WorkerTask) über socket kommt
		// asynchron folgende methode aufrufen (es koennte ja noch mehr kommen)
		executeTask(null);
	}
	
	private void executeTask(WorkerTask task) {
		SocketContext ctx = new SocketContext();
		task.runTask(ctx);
	}
	

}

class SocketContext implements Context {

	@Override
	public void emitIntermediateMapResult(String key, String value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emit(String result) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<KeyValuePair> getMapResult() throws ComputationStoppedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void replaceMapResult(List<KeyValuePair> afterCombining)
			throws ComputationStoppedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}
	
	
}