package ch.zhaw.mapreduce.plugins.thread;

import ch.zhaw.mapreduce.Worker;

import com.google.inject.AbstractModule;

public class ThreadConfig extends AbstractModule {

	@Override
	protected void configure() {
		bind(Worker.class).to(ThreadWorker.class);
	}

}
