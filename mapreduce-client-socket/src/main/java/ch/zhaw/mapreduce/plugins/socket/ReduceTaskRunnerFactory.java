package ch.zhaw.mapreduce.plugins.socket;

import java.util.List;

import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.ReduceInstruction;

import com.google.inject.assistedinject.Assisted;

public interface ReduceTaskRunnerFactory {

	TaskRunner createReduceTaskRunner(@Assisted("mapReduceTaskUuid") String mapReduceTaskUuid,
			@Assisted("taskUuid") String taskUuid, ReduceInstruction redInstr, @Assisted("key") String key,
			List<KeyValuePair> values);

}
