package ch.zhaw.mapreduce.plugins.socket;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.plugins.socket.impl.MapTaskRunner;

import com.google.inject.assistedinject.Assisted;

public interface MapTaskRunnerFactory {

	MapTaskRunner createMapTaskRunner(@Assisted("taskUuid") String taskUuid, MapInstruction mapInstr,
			CombinerInstruction combInstr, @Assisted("input") String input);

}
