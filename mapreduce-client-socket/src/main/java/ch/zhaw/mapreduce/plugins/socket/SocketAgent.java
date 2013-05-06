package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.WorkerTask;

import com.google.inject.assistedinject.Assisted;

public class SocketAgent implements ClientCallback {
	
	private static final long serialVersionUID = -3533183459662782689L;
	
	private static final Logger LOG = Logger.getLogger(SocketAgent.class.getName());
	
	private final ContextFactory ctxFactory;
	
	private final String clientIp;
	
	@Inject
	public SocketAgent(@Assisted String clientIp, ContextFactory ctxFactory) {
		this.clientIp = clientIp;
		this.ctxFactory = ctxFactory;
	}

	@Override
	public void helloslave() {
		LOG.info("Successfully registered on Master");
	}

	@Override
	public Object runTask(WorkerTask task) {
		Context ctx = this.ctxFactory.createContext(task.getMapReduceTaskUUID(), task.getUUID());
		// TODO how to tell the master we've failed?
		task.runTask(ctx);
		List<KeyValuePair> res = ctx.getMapResult();
		if (res != null) {
			return res;
		} else {
			return ctx.getReduceResult();
		}
	}

	@Override
	public String getIp() {
		return this.clientIp;
	}
}
