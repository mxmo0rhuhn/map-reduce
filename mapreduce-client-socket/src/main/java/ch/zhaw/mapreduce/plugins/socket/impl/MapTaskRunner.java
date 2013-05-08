package ch.zhaw.mapreduce.plugins.socket.impl;

import java.util.List;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;

import ch.zhaw.mapreduce.CombinerInstruction;
import ch.zhaw.mapreduce.Context;
import ch.zhaw.mapreduce.ContextFactory;
import ch.zhaw.mapreduce.KeyValuePair;
import ch.zhaw.mapreduce.MapInstruction;
import ch.zhaw.mapreduce.plugins.socket.SocketTaskResult;
import ch.zhaw.mapreduce.plugins.socket.SocketTaskResultFactory;
import ch.zhaw.mapreduce.plugins.socket.TaskRunner;

import com.google.inject.assistedinject.Assisted;

/**
 * Führt eine MapInstruction mit deren Input aus.
 * 
 * @author Reto Hablützel (rethab)
 * 
 */
public final class MapTaskRunner implements TaskRunner {

	private static final Logger LOG = Logger.getLogger(MapTaskRunner.class.getName());

	private final String mapReduceTaskUuid;

	private final String taskUuid;

	private final MapInstruction mapInstr;

	// optional. null wenn nicht vorhanden
	private final CombinerInstruction combInstr;

	private final String input;

	private final ContextFactory ctxFactory;

	private final SocketTaskResultFactory strFactory;

	@Inject
	MapTaskRunner(@Assisted("mapReduceTaskUuid") String mapReduceTaskUuid, @Assisted("taskUuid") String taskUuid,
			@Assisted MapInstruction mapInstr, @Assisted @Nullable CombinerInstruction combInstr,
			@Assisted("input") String input, ContextFactory ctxFactory, SocketTaskResultFactory strFactory) {
		this.mapReduceTaskUuid = mapReduceTaskUuid;
		this.taskUuid = taskUuid;
		this.mapInstr = mapInstr;
		this.combInstr = combInstr;
		this.input = input;
		this.ctxFactory = ctxFactory;
		this.strFactory = strFactory;
	}

	/**
	 * Führt den Task aus und kombiniert die Resultate wenn eine Combiner-Instruction existiert.
	 */
	@Override
	public SocketTaskResult runTask() {
		LOG.entering(MapTaskRunner.class.getName(), "runTask");
		Context ctx = this.ctxFactory.createContext(this.mapReduceTaskUuid, this.taskUuid);
		try {
			// Mappen
			this.mapInstr.map(ctx, input);

			// Alle Ergebnisse verdichten.
			List<KeyValuePair> mapResult = ctx.getMapResult();
			if (this.combInstr != null) {
				mapResult = this.combInstr.combine(mapResult.iterator());
			}
			return this.strFactory.createSuccessResult(mapResult);
		} catch (Exception e) {
			return this.strFactory.createFailureResult(e);
		} finally {
			LOG.exiting(MapTaskRunner.class.getName(), "runTask");
		}
	}

}
