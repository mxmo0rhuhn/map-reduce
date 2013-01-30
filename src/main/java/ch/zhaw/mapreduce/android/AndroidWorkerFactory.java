package ch.zhaw.mapreduce.android;

import com.google.inject.assistedinject.Assisted;

public interface AndroidWorkerFactory {

	AndroidWorker createWorker(@Assisted("clientID") String clientID, @Assisted("device") String deviceID);

}
