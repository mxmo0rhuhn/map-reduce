package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.Worker;

public interface SocketWorkerFactory {

	Worker createSocketWorker(ClientCallback callback);

}
