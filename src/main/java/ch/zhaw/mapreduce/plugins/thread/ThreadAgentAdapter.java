package ch.zhaw.mapreduce.plugins.thread;

import java.util.concurrent.Executors;

import javax.inject.Inject;

import ch.zhaw.mapreduce.Pool;
import ch.zhaw.mapreduce.plugins.AdapterException;
import ch.zhaw.mapreduce.plugins.AgentAdapter;

public class ThreadAgentAdapter implements AgentAdapter {
	
	private final Pool pool;
	
	@Inject
	public ThreadAgentAdapter(Pool pool) {
		this.pool = pool;
	}

	@Override
	public void start() throws AdapterException {
		this.pool.donateWorker(new ThreadWorker(pool, Executors.newSingleThreadExecutor()));
	}

}
